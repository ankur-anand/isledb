package isledb

import (
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"time"
)

const (
	deletionPlanDeadlineLayout = "20060102T150405.000000000Z"
	deletionPlanSHA256Bytes    = 32
	deletionPlanSHA256HexBytes = deletionPlanSHA256Bytes * 2
)

// reclamationLaneSchedule is produced by a reclaim pass and consumed directly
// by the goroutine that ran it. observedAt comes from the same clock that the
// pass used to classify deadlines, so tests and production never mix clocks.
type reclamationLaneSchedule struct {
	observedAt time.Time
	nextDue    time.Time
	idle       bool
}

func earlierReclamationDeadline(left, right time.Time) time.Time {
	if left.IsZero() || (!right.IsZero() && right.Before(left)) {
		return right
	}
	return left
}

func mergeReclamationLaneSchedules(left, right reclamationLaneSchedule) reclamationLaneSchedule {
	observedAt := left.observedAt
	if right.observedAt.After(observedAt) {
		observedAt = right.observedAt
	}
	return reclamationLaneSchedule{
		observedAt: observedAt,
		nextDue:    earlierReclamationDeadline(left.nextDue, right.nextDue),
		idle:       left.idle && right.idle,
	}
}

func deletionPlanReadyName(notBefore time.Time, planID string) string {
	return notBefore.UTC().Format(deletionPlanDeadlineLayout) + "-" + planID + ".json"
}

func parseDeletionPlanReadyName(objectPath string) (time.Time, string, error) {
	name := path.Base(objectPath)
	deadlineBytes := len(deletionPlanDeadlineLayout)
	wantBytes := deadlineBytes + 1 + deletionPlanSHA256HexBytes + len(".json")
	if len(name) != wantBytes || name[deadlineBytes] != '-' || !strings.HasSuffix(name, ".json") {
		return time.Time{}, "", fmt.Errorf("invalid deletion plan ready name %q", name)
	}
	deadlineText := name[:deadlineBytes]
	deadline, err := time.Parse(deletionPlanDeadlineLayout, deadlineText)
	if err != nil || deadline.UTC().Format(deletionPlanDeadlineLayout) != deadlineText {
		return time.Time{}, "", fmt.Errorf("invalid deletion plan ready deadline %q", deadlineText)
	}
	planID := name[deadlineBytes+1 : len(name)-len(".json")]
	decoded, err := hex.DecodeString(planID)
	if err != nil || len(decoded) != deletionPlanSHA256Bytes || strings.ToLower(planID) != planID {
		return time.Time{}, "", fmt.Errorf("invalid deletion plan ready ID %q", planID)
	}
	return deadline.UTC(), planID, nil
}

func validateDeletionPlanObjectPath(
	objectPath string,
	canonicalPath string,
	readyPath string,
) error {
	if objectPath == canonicalPath || objectPath == readyPath {
		return nil
	}
	return fmt.Errorf("deletion plan path mismatch %q", objectPath)
}

func nextReclamationDelay(
	baseInterval time.Duration,
	idleDelay time.Duration,
	now time.Time,
	nextDue time.Time,
	idle bool,
) (time.Duration, time.Duration) {
	if baseInterval <= 0 {
		baseInterval = time.Second
	}
	maxIdle := max(defaultReclaimIdleMaxInterval, baseInterval)
	if !nextDue.IsZero() && nextDue.After(now) {
		return min(nextDue.Sub(now), maxIdle), baseInterval
	}
	if !idle {
		return baseInterval, baseInterval
	}
	if idleDelay < baseInterval {
		idleDelay = baseInterval
	}
	delay := min(idleDelay, maxIdle)
	if idleDelay < maxIdle {
		if idleDelay > maxIdle/2 {
			idleDelay = maxIdle
		} else {
			idleDelay *= 2
		}
	}
	return delay, idleDelay
}

func nextReclamationErrorDelay(
	baseInterval time.Duration,
	errorDelay time.Duration,
	now time.Time,
	nextDue time.Time,
) (time.Duration, time.Duration) {
	if baseInterval <= 0 {
		baseInterval = time.Second
	}
	maxDelay := max(defaultReclaimIdleMaxInterval, baseInterval)
	if errorDelay < baseInterval {
		errorDelay = baseInterval
	}
	delay := min(errorDelay, maxDelay)
	if !nextDue.IsZero() && nextDue.After(now) {
		delay = min(delay, nextDue.Sub(now))
	}
	if errorDelay < maxDelay {
		if errorDelay > maxDelay/2 {
			errorDelay = maxDelay
		} else {
			errorDelay *= 2
		}
	}
	return delay, errorDelay
}
