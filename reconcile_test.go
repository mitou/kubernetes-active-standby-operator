package main

import (
	"testing"
	"time"
)

func testCfg() Config {
	return Config{
		RoleLabelKey:       "active-standby-role",
		RoleLabelValue:     "active",
		ProbeLatencyBudget: 2 * time.Second,
		FailureThreshold:   3,
		FailoverCooldown:   60 * time.Second,
		DeleteStuckActive:  true,
	}
}

func okProbe() ProbeResult  { return ProbeResult{OK: true, Latency: 100 * time.Millisecond} }
func badProbe() ProbeResult { return ProbeResult{OK: false, Latency: 50 * time.Millisecond} }

func TestDecide_HealthyActiveSticks(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Probe: okProbe()},
		{Name: "b", IP: "1.1.1.2", Ready: true, Phase: "Running", Probe: okProbe()},
	}
	dec, fc := decide(pods, map[string]int{}, time.Now(), time.Time{}, cfg)
	if dec.kind != intentNone {
		t.Fatalf("expected intentNone, got %v (%s)", dec.kind, dec.message)
	}
	if fc["a"] != 0 {
		t.Fatalf("expected failCount reset, got %d", fc["a"])
	}
}

func TestDecide_BootstrapWhenNoActive(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "b", IP: "1.1.1.2", Ready: true, Phase: "Running", Probe: okProbe()},
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", Probe: okProbe()},
	}
	dec, _ := decide(pods, map[string]int{}, time.Now(), time.Time{}, cfg)
	if dec.kind != intentBootstrap {
		t.Fatalf("expected intentBootstrap, got %v", dec.kind)
	}
	if dec.target != "a" { // deterministic lowest name
		t.Fatalf("expected target a, got %s", dec.target)
	}
}

func TestDecide_HysteresisBeforeFailover(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Probe: badProbe()},
		{Name: "b", IP: "1.1.1.2", Ready: true, Phase: "Running", Probe: okProbe()},
	}
	fc := map[string]int{}
	now := time.Now()
	// First two failures stay within the threshold.
	for i := 1; i < cfg.FailureThreshold; i++ {
		var dec decision
		dec, fc = decide(pods, fc, now, time.Time{}, cfg)
		if dec.kind != intentNone {
			t.Fatalf("failure %d: expected intentNone, got %v", i, dec.kind)
		}
		if fc["a"] != i {
			t.Fatalf("failure %d: expected failCount %d, got %d", i, i, fc["a"])
		}
	}
	// The threshold-th failure triggers failover.
	dec, _ := decide(pods, fc, now, time.Time{}, cfg)
	if dec.kind != intentFailover {
		t.Fatalf("expected intentFailover, got %v (%s)", dec.kind, dec.message)
	}
	if dec.target != "b" || dec.current != "a" {
		t.Fatalf("expected a->b, got %s->%s", dec.current, dec.target)
	}
	if !dec.deleteCurrent {
		t.Fatalf("expected deleteCurrent for a Running stuck pod")
	}
}

func TestDecide_CooldownSuppressesFailover(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Probe: badProbe()},
		{Name: "b", IP: "1.1.1.2", Ready: true, Phase: "Running", Probe: okProbe()},
	}
	now := time.Now()
	fc := map[string]int{"a": cfg.FailureThreshold}                // already past threshold
	dec, _ := decide(pods, fc, now, now.Add(-10*time.Second), cfg) // last failover 10s ago
	if dec.kind != intentNone {
		t.Fatalf("expected cooldown to suppress failover, got %v", dec.kind)
	}
}

func TestDecide_HoldWhenNoHealthyStandby(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Probe: badProbe()},
		{Name: "b", IP: "1.1.1.2", Ready: false, Phase: "Running", Probe: badProbe()},
	}
	fc := map[string]int{"a": cfg.FailureThreshold}
	dec, _ := decide(pods, fc, time.Now(), time.Time{}, cfg)
	if dec.kind != intentHold {
		t.Fatalf("expected intentHold, got %v (%s)", dec.kind, dec.message)
	}
}

func TestDecide_TerminatingActivePromotesImmediately(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Deleting: true, Probe: okProbe()},
		{Name: "b", IP: "1.1.1.2", Ready: true, Phase: "Running", Probe: okProbe()},
	}
	dec, _ := decide(pods, map[string]int{}, time.Now(), time.Time{}, cfg)
	if dec.kind != intentFailover {
		t.Fatalf("expected immediate intentFailover, got %v", dec.kind)
	}
	if dec.target != "b" {
		t.Fatalf("expected promote b, got %s", dec.target)
	}
	if dec.deleteCurrent {
		t.Fatalf("did not expect to delete an already-terminating pod")
	}
}

func TestDecide_SplitBrainResolves(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Probe: okProbe()},
		{Name: "b", IP: "1.1.1.2", Ready: false, Phase: "Running", IsActive: true, Probe: badProbe()},
	}
	dec, _ := decide(pods, map[string]int{}, time.Now(), time.Time{}, cfg)
	if dec.kind != intentResolveSplitBrain {
		t.Fatalf("expected intentResolveSplitBrain, got %v", dec.kind)
	}
	if dec.target != "a" { // healthy one kept
		t.Fatalf("expected keep a, got %s", dec.target)
	}
	if len(dec.losers) != 1 || dec.losers[0] != "b" {
		t.Fatalf("expected demote [b], got %v", dec.losers)
	}
}

func TestDecide_SlowActiveCountsAsUnhealthy(t *testing.T) {
	cfg := testCfg()
	slow := ProbeResult{OK: true, Latency: cfg.ProbeLatencyBudget + time.Second}
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Probe: slow},
		{Name: "b", IP: "1.1.1.2", Ready: true, Phase: "Running", Probe: okProbe()},
	}
	// A probe that is OK but over the latency budget must increment failCount,
	// not be treated as healthy.
	_, fc := decide(pods, map[string]int{}, time.Now(), time.Time{}, cfg)
	if fc["a"] != 1 {
		t.Fatalf("expected slow active to increment failCount, got %d", fc["a"])
	}
}

func TestDecide_ZeroPodsHolds(t *testing.T) {
	cfg := testCfg()
	dec, _ := decide(nil, map[string]int{}, time.Now(), time.Time{}, cfg)
	if dec.kind != intentHold {
		t.Fatalf("expected intentHold for zero pods, got %v", dec.kind)
	}
}

func TestDecide_TerminatingPromotionDoesNotArmCooldown(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Deleting: true, Probe: okProbe()},
		{Name: "b", IP: "1.1.1.2", Ready: true, Phase: "Running", Probe: okProbe()},
	}
	dec, _ := decide(pods, map[string]int{}, time.Now(), time.Time{}, cfg)
	if dec.kind != intentFailover {
		t.Fatalf("expected intentFailover, got %v", dec.kind)
	}
	if dec.recordFailover {
		t.Fatalf("a terminating-active promotion must not arm the cooldown")
	}
}

func TestDecide_HealthFailoverArmsCooldown(t *testing.T) {
	cfg := testCfg()
	pods := []podState{
		{Name: "a", IP: "1.1.1.1", Ready: true, Phase: "Running", IsActive: true, Probe: badProbe()},
		{Name: "b", IP: "1.1.1.2", Ready: true, Phase: "Running", Probe: okProbe()},
	}
	fc := map[string]int{"a": cfg.FailureThreshold - 1}
	dec, _ := decide(pods, fc, time.Now(), time.Time{}, cfg)
	if dec.kind != intentFailover || !dec.recordFailover {
		t.Fatalf("expected a cooldown-arming health failover, got kind=%v record=%v", dec.kind, dec.recordFailover)
	}
}
