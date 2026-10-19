package isledb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func listSSTMergeIntents(t *testing.T, store *blobstore.Store) []sstMergeIntent {
	t.Helper()
	ctx := context.Background()
	listed, err := store.List(ctx, blobstore.ListOptions{Prefix: sstMergeIntentPrefix + "/"})
	if err != nil {
		t.Fatalf("list intents: %v", err)
	}
	var intents []sstMergeIntent
	for _, object := range listed.Objects {
		if object.IsDir {
			continue
		}
		payload, _, err := store.Read(ctx, object.Key)
		if err != nil {
			t.Fatalf("read intent %s: %v", object.Key, err)
		}
		intent, err := decodeSSTMergeIntent(payload)
		if err != nil {
			t.Fatalf("decode intent %s: %v", object.Key, err)
		}
		intents = append(intents, intent)
	}
	return intents
}

func objectExists(t *testing.T, store *blobstore.Store, key string) bool {
	t.Helper()
	_, _, err := store.Read(context.Background(), key)
	if errors.Is(err, blobstore.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return true
}

func TestWriterL0Merge_LandedMergeRetiresItsInputs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-retire-inputs")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	// Two quick flushes land in one run; the age limit then closes it.
	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(64<<20, 500*time.Millisecond))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	merges := make(chan publishedMerge, 8)
	w.onL0MergePublished = func(merged mergedL0Run, err error) { merges <- publishedMerge{merged, err} }

	for _, k := range []string{"a", "b"} {
		if _, err := w.put(ctx, []byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	time.Sleep(600 * time.Millisecond)
	published := waitForL0Merge(t, w, merges)
	if published.err != nil {
		t.Fatalf("merge refused: %v", published.err)
	}
	merged := published.merged
	// The pass that published also wrote the plan; one more pass is harmless
	// and proves the queue is empty.
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if w.l0Merge.nextRetirement() != nil {
		t.Fatalf("retirement still queued after the pass")
	}

	// The intent: settled for the inputs, naming the plan.
	intents := listSSTMergeIntents(t, store)
	if len(intents) != 1 {
		t.Fatalf("intents=%d, want 1", len(intents))
	}
	intent := intents[0]
	if intent.IntentID != sstMergeIntentID(merged.output.ID) || intent.Output.ID != merged.output.ID ||
		len(intent.Inputs) != len(merged.inputs) || intent.WriterEpoch != w.fenceToken.Epoch {
		t.Fatalf("intent=%+v", intent)
	}
	if intent.Settled != sstDeletionOutcomeInputs || intent.SettledPlanID == "" || intent.SettledAt.IsZero() {
		t.Fatalf("intent not settled for inputs: %+v", intent)
	}

	// The plan: the inputs as targets, the writer as source, the deadline a
	// pinned-view age past the commit.
	plans := listSSTDeletionPlans(t, ctx, store)
	if len(plans) != 1 {
		t.Fatalf("plans=%d, want 1", len(plans))
	}
	plan := plans[0]
	if plan.PlanID != intent.SettledPlanID {
		t.Fatalf("plan %s, intent names %s", plan.PlanID, intent.SettledPlanID)
	}
	if plan.Source.Kind != sstDeletionSourceWriterL0Merge || plan.Source.IntentID != intent.IntentID ||
		plan.Source.Outcome != sstDeletionOutcomeInputs || plan.Source.Epoch != w.fenceToken.Epoch ||
		plan.Source.CommandID != "" {
		t.Fatalf("plan source=%+v", plan.Source)
	}
	if len(plan.Targets) != len(merged.inputs) {
		t.Fatalf("plan targets=%d, inputs=%d", len(plan.Targets), len(merged.inputs))
	}
	for i, input := range merged.inputs {
		if plan.Targets[i].ID != input.ID || plan.Targets[i].Key != store.SSTPath(input.ID) || plan.Targets[i].Size != input.Size {
			t.Fatalf("target %d=%+v, input=%+v", i, plan.Targets[i], input)
		}
	}
	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if plan.PinnedViewAge != current.PinnedViewAge() || plan.SafetyMargin != defaultSSTDeletionSafetyMargin {
		t.Fatalf("plan timing=%+v", plan)
	}
	if plan.AppliedAt.IsZero() || plan.AppliedAt.After(plan.ObservedAt) ||
		plan.NotBefore.Before(plan.ObservedAt.Add(plan.PinnedViewAge)) {
		t.Fatalf("plan deadline: applied=%s observed=%s not_before=%s", plan.AppliedAt, plan.ObservedAt, plan.NotBefore)
	}
	if exists := objectExists(t, store, sstDeletionPlanCanonicalPath(store, plan.PlanID)); !exists {
		t.Fatalf("canonical plan record missing")
	}

	// Before the deadline the lane deletes nothing. After it, the inputs go
	// and the output stays.
	for _, input := range merged.inputs {
		if !objectExists(t, store, store.SSTPath(input.ID)) {
			t.Fatalf("input %s deleted before its deadline", input.ID)
		}
	}
	if _, err := runSSTDeletionPlanReclaimer(ctx, store, 128, 1024, time.Now().UTC()); err != nil {
		t.Fatalf("reclaim before deadline: %v", err)
	}
	for _, input := range merged.inputs {
		if !objectExists(t, store, store.SSTPath(input.ID)) {
			t.Fatalf("input %s deleted before its deadline", input.ID)
		}
	}
	stats, err := runSSTDeletionPlanReclaimer(ctx, store, 128, 1024, plan.NotBefore.Add(time.Second))
	if err != nil {
		t.Fatalf("reclaim after deadline: %v", err)
	}
	if stats.Deleted != len(merged.inputs) {
		t.Fatalf("reclaim stats=%+v, want %d deleted", stats, len(merged.inputs))
	}
	for _, input := range merged.inputs {
		if objectExists(t, store, store.SSTPath(input.ID)) {
			t.Fatalf("input %s still in storage after its deadline", input.ID)
		}
	}
	if !objectExists(t, store, store.SSTPath(merged.output.ID)) {
		t.Fatalf("output deleted")
	}
	if remaining := listSSTDeletionPlans(t, ctx, store); len(remaining) != 0 {
		t.Fatalf("plans left after reclaim: %d", len(remaining))
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestWriterL0Merge_RefusedMergeRetiresItsOutput(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-retire-output")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(64<<20, time.Hour))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)
	merges := make(chan publishedMerge, 8)
	w.onL0MergePublished = func(merged mergedL0Run, err error) { merges <- publishedMerge{merged, err} }

	if _, err := w.put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// A merge whose inputs are gone from L0, with its intent and an uploaded
	// output, as a drain between merge and publish leaves behind.
	inputs := []sstMetadata{{ID: "gone-1.sst", Size: 10}, {ID: "gone-2.sst", Size: 10}}
	output := sstMetadata{ID: "compacted-" + strings.Repeat("0", 64) + "-0001.sst", SeqLo: 1, SeqHi: 2, Size: 10, Epoch: 1}
	if _, err := store.Write(ctx, store.SSTPath(output.ID), []byte("0123456789")); err != nil {
		t.Fatalf("write output: %v", err)
	}
	intent, err := w.l0Merge.writeL0MergeIntent(ctx, inputs, output.ID)
	if err != nil {
		t.Fatalf("writeL0MergeIntent: %v", err)
	}
	w.l0Merge.mu.Lock()
	w.l0Merge.finished = append(w.l0Merge.finished, mergedL0Run{commitID: "stale", intent: intent, inputs: inputs, output: output})
	w.l0Merge.mu.Unlock()

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case published := <-merges:
		if !errors.Is(published.err, manifest.ErrInvalidManifest) {
			t.Fatalf("settled with %v", published.err)
		}
	default:
		t.Fatalf("refused merge not settled")
	}

	intents := listSSTMergeIntents(t, store)
	if len(intents) != 1 || intents[0].Settled != sstDeletionOutcomeOutput || intents[0].SettledPlanID == "" {
		t.Fatalf("intents=%+v", intents)
	}
	plans := listSSTDeletionPlans(t, ctx, store)
	if len(plans) != 1 || plans[0].PlanID != intents[0].SettledPlanID {
		t.Fatalf("plans=%+v", plans)
	}
	plan := plans[0]
	if plan.Source.Outcome != sstDeletionOutcomeOutput || len(plan.Targets) != 1 || plan.Targets[0].ID != output.ID {
		t.Fatalf("plan=%+v", plan)
	}
	stats, err := runSSTDeletionPlanReclaimer(ctx, store, 128, 1024, plan.NotBefore.Add(time.Second))
	if err != nil || stats.Deleted != 1 {
		t.Fatalf("reclaim stats=%+v err=%v", stats, err)
	}
	if objectExists(t, store, store.SSTPath(output.ID)) {
		t.Fatalf("refused output still in storage")
	}
}

func TestL0Merger_DroppedMergeRetiresItsOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := blobstore.NewMemory("writer-l0-merge-drop-retire")
	defer store.Close()
	w := &writer{ctx: ctx, store: store}
	m := newL0Merger(w, WriterL0MergeOptions{TargetBytes: 100, MaxAge: time.Hour})
	output := sstMetadata{ID: "compacted-out", Size: 7}
	m.finished = append(m.finished, mergedL0Run{
		commitID: "m-xy",
		intent:   sstMergeIntent{IntentID: "intent-xy"},
		inputs:   []sstMetadata{{ID: "x"}, {ID: "y"}},
		output:   output,
	})
	if dropped := m.dropTouching([]string{"x"}); dropped != 1 {
		t.Fatalf("dropped=%d", dropped)
	}
	retirement := m.nextRetirement()
	if retirement == nil || retirement.outcome != sstDeletionOutcomeOutput || retirement.intent.IntentID != "intent-xy" ||
		len(retirement.targets) != 1 || retirement.targets[0].ID != output.ID || retirement.targets[0].Key != store.SSTPath(output.ID) {
		t.Fatalf("retirement=%+v", retirement)
	}
	m.retirementDone("intent-xy")
	if m.nextRetirement() != nil {
		t.Fatalf("retirement not removed")
	}
}

func TestSSTMergeIntentRoundTrip(t *testing.T) {
	store := blobstore.NewMemory("intent-codec")
	defer store.Close()
	outputID := "compacted-" + strings.Repeat("a", 64) + "-0001.sst"
	intent := sstMergeIntent{
		Version:     sstMergeIntentVersion,
		Kind:        sstMergeIntentKind,
		IntentID:    sstMergeIntentID(outputID),
		WriterEpoch: 3,
		CreatedAt:   time.Unix(1000, 0).UTC(),
		Inputs:      []sstDeletionTarget{{ID: "1-1-2-3.sst", Key: store.SSTPath("1-1-2-3.sst"), Size: 5}},
		Output:      sstDeletionTarget{ID: outputID, Key: store.SSTPath(outputID)},
	}
	payload, err := encodeSSTMergeIntent(intent)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := decodeSSTMergeIntent(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.IntentID != intent.IntentID || len(decoded.Inputs) != 1 || decoded.Output.ID != outputID || decoded.Settled != "" {
		t.Fatalf("decoded=%+v", decoded)
	}

	bad := intent
	bad.IntentID = "wrong"
	if _, err := encodeSSTMergeIntent(bad); err == nil {
		t.Fatalf("mismatched intent id accepted")
	}
	bad = intent
	bad.Settled = sstDeletionOutcomeInputs
	if _, err := encodeSSTMergeIntent(bad); err == nil {
		t.Fatalf("settled without a plan accepted")
	}
	bad = intent
	bad.SettledPlanID = "plan"
	if _, err := encodeSSTMergeIntent(bad); err == nil {
		t.Fatalf("plan without an outcome accepted")
	}
	settled := intent
	settled.Settled = sstDeletionOutcomeOutput
	settled.SettledPlanID = "plan"
	settled.SettledAt = time.Unix(2000, 0).UTC()
	if _, err := encodeSSTMergeIntent(settled); err != nil {
		t.Fatalf("settled intent rejected: %v", err)
	}
}

func TestSSTDeletionPlanSourceValidation(t *testing.T) {
	store := blobstore.NewMemory("plan-source")
	defer store.Close()
	target := sstDeletionTarget{ID: "x.sst", Key: store.SSTPath("x.sst"), Size: 1}
	now := time.Now().UTC()
	for name, c := range map[string]struct {
		source sstDeletionPlanSource
		ok     bool
	}{
		"maintenance":               {sstDeletionPlanSource{CommandID: "c", Epoch: 1, Generation: 1}, true},
		"maintenance no generation": {sstDeletionPlanSource{CommandID: "c", Epoch: 1}, false},
		"writer":                    {sstDeletionPlanSource{Kind: sstDeletionSourceWriterL0Merge, IntentID: "i", Epoch: 1, Outcome: sstDeletionOutcomeInputs}, true},
		"writer output":             {sstDeletionPlanSource{Kind: sstDeletionSourceWriterL0Merge, IntentID: "i", Epoch: 1, Outcome: sstDeletionOutcomeOutput}, true},
		"writer no intent":          {sstDeletionPlanSource{Kind: sstDeletionSourceWriterL0Merge, Epoch: 1, Outcome: sstDeletionOutcomeInputs}, false},
		"writer bad outcome":        {sstDeletionPlanSource{Kind: sstDeletionSourceWriterL0Merge, IntentID: "i", Epoch: 1, Outcome: "both"}, false},
		"writer with command":       {sstDeletionPlanSource{Kind: sstDeletionSourceWriterL0Merge, IntentID: "i", Epoch: 1, Outcome: sstDeletionOutcomeInputs, CommandID: "c"}, false},
		"maintenance with intent":   {sstDeletionPlanSource{CommandID: "c", Epoch: 1, Generation: 1, IntentID: "i"}, false},
		"unknown kind":              {sstDeletionPlanSource{Kind: "other", Epoch: 1}, false},
	} {
		_, _, err := newSSTDeletionPlan(store, c.source, now, now, time.Minute, 0, []sstDeletionTarget{target})
		if (err == nil) != c.ok {
			t.Fatalf("%s: err=%v, want ok=%v", name, err, c.ok)
		}
	}
}
