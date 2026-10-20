package isledb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// writeIntentForTest records an intent as a writer with fence epoch would,
// naming inputs and output, and optionally settles it.
func writeIntentForTest(t *testing.T, store *blobstore.Store, epoch uint64, inputs []sstMetadata, outputID, settled string) sstMergeIntent {
	t.Helper()
	intent := sstMergeIntent{
		Version:     sstMergeIntentVersion,
		Kind:        sstMergeIntentKind,
		IntentID:    sstMergeIntentID(outputID),
		WriterEpoch: epoch,
		CreatedAt:   time.Now().UTC(),
		Inputs:      sstDeletionTargets(store, inputs),
		Output:      sstDeletionTarget{ID: outputID, Key: store.SSTPath(outputID)},
	}
	if settled != "" {
		intent.Settled = settled
		intent.SettledPlanID = "plan-" + settled
		intent.SettledAt = time.Now().UTC()
	}
	payload, err := encodeSSTMergeIntent(intent)
	if err != nil {
		t.Fatalf("encode intent: %v", err)
	}
	if _, err := store.Write(context.Background(), sstMergeIntentPath(store, intent.IntentID), payload); err != nil {
		t.Fatalf("write intent: %v", err)
	}
	return intent
}

func mergedIDForTest(seed byte) string {
	return "compacted-" + strings.Repeat(string(rune('a'+seed)), 64) + "-0001.sst"
}

func TestSSTIntentAuditRemovesSettledAndLeavesTheLiveWritersOwn(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("intent-audit-settled")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)
	if _, err := w.put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	l0 := replayManifestForTest(t, ctx, store).L0SSTs
	epoch := w.fenceToken.Epoch

	settled := writeIntentForTest(t, store, epoch, l0, mergedIDForTest(0), sstDeletionOutcomeInputs)
	pending := writeIntentForTest(t, store, epoch, l0, mergedIDForTest(1), "")
	cleaner := newSSTCleaner(store, sstCleanerOptions{ManifestLog: manifestStore})
	stats, err := cleaner.runIntentAudit(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("runIntentAudit: %v", err)
	}
	if stats.IntentsScanned != 2 || stats.IntentsSettled != 1 || stats.IntentsResolved != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	left := listSSTMergeIntents(t, store)
	if len(left) != 1 || left[0].IntentID != pending.IntentID {
		t.Fatalf("intents left=%+v, want only the live writer's %s", left, pending.IntentID)
	}
	if objectExists(t, store, sstMergeIntentPath(store, settled.IntentID)) {
		t.Fatalf("settled intent still stored")
	}
	if plans := listSSTDeletionPlans(t, ctx, store); len(plans) != 0 {
		t.Fatalf("audit wrote plans for a live writer's intent: %+v", plans)
	}
}

func TestSSTIntentAuditResolvesADeadWritersIntents(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("intent-audit-dead")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	first, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	for _, k := range []string{"a", "b"} {
		if _, err := first.put(ctx, []byte(k), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := first.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	l0 := replayManifestForTest(t, ctx, store).L0SSTs // b, a
	oldEpoch := first.fenceToken.Epoch

	// Merge that never landed: inputs live, output uploaded but unreferenced.
	notLanded := mergedIDForTest(2)
	if _, err := store.Write(ctx, store.SSTPath(notLanded), []byte("orphan output")); err != nil {
		t.Fatalf("write output: %v", err)
	}
	intentNotLanded := writeIntentForTest(t, store, oldEpoch, l0, notLanded, "")

	// Merge that landed, then its writer died before planning: inputs gone
	// from the manifest, objects still there, output live.
	goneInputs := []sstMetadata{{ID: "9-1-2-3.sst", Size: 4}, {ID: "9-3-4-5.sst", Size: 4}}
	for _, sst := range goneInputs {
		if _, err := store.Write(ctx, store.SSTPath(sst.ID), []byte("gone")); err != nil {
			t.Fatalf("write input: %v", err)
		}
	}
	landed := mergedIDForTest(3)
	landedMeta := flushLikeSST(landed, 1, 20)
	if _, err := manifestStore.AppendWriterCommit(ctx, manifest.WriterCommit{ID: "fake-landed", SSTable: landedMeta}); err != nil {
		t.Fatalf("append landed output: %v", err)
	}
	intentLanded := writeIntentForTest(t, store, oldEpoch, goneInputs, landed, "")

	// The first writer dies; a successor claims the fence.
	first.stop()
	second, err := newWriter(ctx, store, newManifestStore(store, nil), testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("successor newWriter: %v", err)
	}
	defer second.close(ctx)
	// The successor's own intent is left alone.
	own := writeIntentForTest(t, store, second.fenceToken.Epoch, l0, mergedIDForTest(4), "")

	cleaner := newSSTCleaner(store, sstCleanerOptions{ManifestLog: newManifestStore(store, nil)})
	now := time.Now().UTC()
	stats, err := cleaner.runIntentAudit(ctx, now)
	if err != nil {
		t.Fatalf("runIntentAudit: %v", err)
	}
	if stats.IntentsScanned != 3 || stats.IntentsResolved != 2 || stats.IntentsSettled != 0 || stats.Failed != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	left := listSSTMergeIntents(t, store)
	if len(left) != 1 || left[0].IntentID != own.IntentID {
		t.Fatalf("intents left=%+v, want only %s", left, own.IntentID)
	}

	plans := listSSTDeletionPlans(t, ctx, store)
	if len(plans) != 2 {
		t.Fatalf("plans=%d, want one per resolved intent: %+v", len(plans), plans)
	}
	byIntent := map[string]sstDeletionPlan{}
	for _, plan := range plans {
		byIntent[plan.Source.IntentID] = plan
	}
	outputPlan := byIntent[intentNotLanded.IntentID]
	if outputPlan.Source.Outcome != sstDeletionOutcomeOutput || len(outputPlan.Targets) != 1 || outputPlan.Targets[0].ID != notLanded ||
		outputPlan.Source.Epoch != oldEpoch {
		t.Fatalf("not-landed plan=%+v", outputPlan)
	}
	inputPlan := byIntent[intentLanded.IntentID]
	if inputPlan.Source.Outcome != sstDeletionOutcomeInputs || len(inputPlan.Targets) != 2 {
		t.Fatalf("landed plan=%+v", inputPlan)
	}
	for _, plan := range plans {
		if plan.NotBefore.Before(now.Add(plan.PinnedViewAge)) {
			t.Fatalf("plan %s due %s, before now plus the pinned-view age", plan.PlanID, plan.NotBefore)
		}
	}
	// Live files were never planned.
	for _, plan := range plans {
		for _, target := range plan.Targets {
			for _, sst := range l0 {
				if target.ID == sst.ID {
					t.Fatalf("live input %s planned for deletion", sst.ID)
				}
			}
			if target.ID == landed {
				t.Fatalf("live output %s planned for deletion", landed)
			}
		}
	}
}

func flushLikeSST(id string, seqLo, seqHi uint64) sstMetadata {
	return sstMetadata{ID: id, Epoch: seqLo, SeqLo: seqLo, SeqHi: seqHi, Size: 10,
		MinKey: []byte("a"), MaxKey: []byte("z"), CreatedAt: time.Now().UTC()}
}

func TestWriterL0Merge_AdoptsAPreviousWritersFlushFiles(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-adopt")
	defer store.Close()

	// A writer with merging on commits two files and closes before merging.
	first, err := newWriter(ctx, store, newManifestStore(store, nil), testL0MergeWriterOptions(64<<20, time.Hour))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	for _, k := range []string{"a", "b"} {
		if _, err := first.put(ctx, []byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := first.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if err := first.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	leftovers := l0IDsOf(t, newManifestStore(store, nil))
	if len(leftovers) != 2 {
		t.Fatalf("L0 after the first writer = %v", leftovers)
	}

	// The next writer takes them over and merges them.
	manifestStore := newManifestStore(store, nil)
	second, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(64<<20, time.Hour))
	if err != nil {
		t.Fatalf("successor newWriter: %v", err)
	}
	merges := make(chan publishedMerge, 8)
	second.onL0MergePublished = func(merged mergedL0Run, err error) { merges <- publishedMerge{merged, err} }
	published := waitForL0Merge(t, second, merges)
	if published.err != nil {
		t.Fatalf("merge refused: %v", published.err)
	}
	merged := published.merged
	if len(merged.inputs) != 2 || merged.inputs[0].ID != leftovers[1] || merged.inputs[1].ID != leftovers[0] {
		t.Fatalf("adopted inputs=%v, want the two leftovers oldest first %v", merged.inputs, leftovers)
	}
	if l0 := l0IDsOf(t, manifestStore); len(l0) != 1 || l0[0] != merged.output.ID {
		t.Fatalf("L0 = %v, want only %s", l0, merged.output.ID)
	}
	entries := readMergedSST(t, store, merged.output)
	if len(entries) != 2 {
		t.Fatalf("merged entries=%d, want 2", len(entries))
	}
	if err := second.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Nothing to adopt behind a merged file: the third writer holds nothing.
	third, err := newWriter(ctx, store, newManifestStore(store, nil), testL0MergeWriterOptions(64<<20, time.Hour))
	if err != nil {
		t.Fatalf("third newWriter: %v", err)
	}
	third.l0Merge.wait()
	if held := third.l0Merge.heldBytes(); held != 0 {
		t.Fatalf("third writer holds %d bytes with nothing raw in L0", held)
	}
	if err := third.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestL0Merger_LeftoverRunStopsAtMergedFilesAndTheHeldLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := newL0Merger(&writer{ctx: ctx}, WriterL0MergeOptions{TargetBytes: 100, MaxAge: time.Hour})
	raw := func(i int, size int64) sstMetadata {
		return sstMetadata{ID: buildSSTIDWithTimestamp(uint64(i), uint64(10*i), uint64(10*i+5), time.Now()), Size: size}
	}
	l0 := []sstMetadata{raw(5, 60), raw(4, 60), raw(3, 60), raw(2, 60), {ID: mergedIDForTest(0), Size: 500}, raw(1, 60)}
	got := m.leftoverL0Run(l0)
	// Oldest first, within two targets: 60+60+60 = 180 fits, a fourth does not.
	if len(got) != 3 || got[0].ID != l0[2].ID || got[2].ID != l0[0].ID {
		t.Fatalf("leftovers=%v", got)
	}
	if got := m.leftoverL0Run([]sstMetadata{{ID: mergedIDForTest(0)}, raw(1, 60)}); len(got) != 0 {
		t.Fatalf("leftovers behind a merged file: %v", got)
	}
}
