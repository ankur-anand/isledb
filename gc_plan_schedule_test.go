package isledb

import (
	"strings"
	"testing"
	"time"
)

func TestDeletionPlanReadyNamesSortAndRoundTripByDeadline(t *testing.T) {
	first := time.Date(2026, time.August, 24, 10, 11, 12, 13, time.UTC)
	second := first.Add(time.Nanosecond)
	firstID := strings.Repeat("a", deletionPlanSHA256HexBytes)
	secondID := strings.Repeat("b", deletionPlanSHA256HexBytes)

	firstName := deletionPlanReadyName(first, firstID)
	secondName := deletionPlanReadyName(second, secondID)
	if firstName >= secondName {
		t.Fatalf("deadline names are not ordered: first=%q second=%q", firstName, secondName)
	}
	gotDeadline, gotID, err := parseDeletionPlanReadyName("prefix/" + firstName)
	if err != nil {
		t.Fatalf("parse ready name: %v", err)
	}
	if !gotDeadline.Equal(first) || gotID != firstID {
		t.Fatalf("parsed deadline=%s id=%q, want deadline=%s id=%q", gotDeadline, gotID, first, firstID)
	}
}

func TestDeletionPlanReadyNameRejectsMalformedOrderingFields(t *testing.T) {
	validID := strings.Repeat("a", deletionPlanSHA256HexBytes)
	tests := []string{
		"not-a-deadline-" + validID + ".json",
		deletionPlanReadyName(time.Now().UTC(), strings.ToUpper(validID)),
		deletionPlanReadyName(time.Now().UTC(), validID) + ".extra",
	}
	for _, name := range tests {
		if _, _, err := parseDeletionPlanReadyName(name); err == nil {
			t.Errorf("parseDeletionPlanReadyName(%q) succeeded", name)
		}
	}
}

func TestNextReclamationDelayBacksOffAndHonorsKnownDeadline(t *testing.T) {
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	base := time.Second

	delay, nextIdle := nextReclamationDelay(base, base, now, time.Time{}, true)
	if delay != base || nextIdle != 2*base {
		t.Fatalf("first idle delay=%s next=%s", delay, nextIdle)
	}
	delay, nextIdle = nextReclamationDelay(base, nextIdle, now, time.Time{}, true)
	if delay != 2*base || nextIdle != 4*base {
		t.Fatalf("second idle delay=%s next=%s", delay, nextIdle)
	}

	dueIn := 250 * time.Millisecond
	delay, nextIdle = nextReclamationDelay(base, time.Minute, now, now.Add(dueIn), false)
	if delay != dueIn || nextIdle != base {
		t.Fatalf("known deadline delay=%s next=%s, want %s and %s", delay, nextIdle, dueIn, base)
	}

	delay, _ = nextReclamationDelay(base, base, now, now.Add(time.Hour), false)
	if delay != defaultReclaimIdleMaxInterval {
		t.Fatalf("safety rescan delay=%s want=%s", delay, defaultReclaimIdleMaxInterval)
	}
}

func TestNextReclamationErrorDelayBacksOffAndHonorsKnownDeadline(t *testing.T) {
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	base := time.Second

	delay, nextError := nextReclamationErrorDelay(base, base, now, time.Time{})
	if delay != base || nextError != 2*base {
		t.Fatalf("first error delay=%s next=%s", delay, nextError)
	}
	delay, nextError = nextReclamationErrorDelay(base, nextError, now, time.Time{})
	if delay != 2*base || nextError != 4*base {
		t.Fatalf("second error delay=%s next=%s", delay, nextError)
	}

	dueIn := 250 * time.Millisecond
	delay, nextError = nextReclamationErrorDelay(base, time.Minute, now, now.Add(dueIn))
	if delay != dueIn || nextError != 2*time.Minute {
		t.Fatalf("deadline-limited error delay=%s next=%s, want %s and %s",
			delay, nextError, dueIn, 2*time.Minute)
	}

	delay, nextError = nextReclamationErrorDelay(
		base, defaultReclaimIdleMaxInterval, now, time.Time{})
	if delay != defaultReclaimIdleMaxInterval || nextError != defaultReclaimIdleMaxInterval {
		t.Fatalf("capped error delay=%s next=%s", delay, nextError)
	}
}
