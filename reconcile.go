package main

import (
	"sort"
	"time"
)

// podState is the flattened view of a managed pod the decision logic needs.
type podState struct {
	Name     string
	IP       string
	Ready    bool // k8s Ready condition (the AND of all containers in the pod)
	Phase    string
	IsActive bool // currently carries the role label
	Deleting bool // DeletionTimestamp != nil
	Probe    ProbeResult
}

// intent enumerates the high-level action the reconcile loop should take.
type intent int

const (
	intentNone              intent = iota // healthy steady state, nothing to do
	intentBootstrap                       // no active pod yet: assign one
	intentFailover                        // move active from current -> target (add-before-remove)
	intentResolveSplitBrain               // more than one active: demote the extras
	intentHold                            // degraded but unfixable: keep last assignment, alert
)

// decision is the pure output of decide(); execute() turns it into API calls.
type decision struct {
	kind          intent
	target        string   // pod to promote / keep
	current       string   // active pod being demoted (failover)
	losers        []string // extra actives to demote (split-brain)
	deleteCurrent bool     // delete `current` after demotion (recycle a stuck pod)
	// recordFailover marks a genuine health-driven failover, which arms the
	// anti-flap cooldown. A promotion triggered merely by a terminating active
	// (rolling update) leaves it false so it neither starts a cooldown nor lets
	// a later real failover be suppressed by one.
	recordFailover bool
	message        string
}

// decide is a pure function: given the current pod states, failure counters and
// timing, it returns the action to take plus the updated failure counters. Being
// pure makes the whole state machine unit-testable without a cluster.
func decide(pods []podState, failCount map[string]int, now, lastFailover time.Time, cfg Config) (decision, map[string]int) {
	fc := cloneCounts(failCount)
	// Drop counters for pods that no longer exist to avoid unbounded growth.
	pruneCounts(fc, pods)

	actives := filterActive(pods)

	// ---- More than one active: split-brain, self-heal down to one. ----
	if len(actives) > 1 {
		keep := bestActive(actives, fc)
		var losers []string
		for _, a := range actives {
			if a.Name != keep.Name {
				losers = append(losers, a.Name)
			}
		}
		return decision{kind: intentResolveSplitBrain, target: keep.Name, losers: losers,
			message: "multiple active pods detected; demoting extras"}, fc
	}

	// ---- Exactly one active. ----
	if len(actives) == 1 {
		a := actives[0]

		// A terminating active (rolling update / eviction / our own delete) must
		// be replaced immediately, without waiting out the failure threshold.
		if a.Deleting {
			sb, ok := pickStandby(pods, a.Name)
			if !ok {
				return decision{kind: intentHold,
					message: "active pod is terminating but no healthy standby is available"}, fc
			}
			return decision{kind: intentFailover, target: sb.Name, current: a.Name,
				message: "active pod terminating; promoting standby"}, fc
		}

		if healthy(a, cfg) {
			fc[a.Name] = 0 // stickiness: a healthy active is never moved
			return decision{kind: intentNone}, fc
		}

		// Unhealthy: apply hysteresis before acting.
		fc[a.Name]++
		if fc[a.Name] < cfg.FailureThreshold {
			return decision{kind: intentNone,
				message: "active probe failing, within hysteresis threshold"}, fc
		}
		if now.Sub(lastFailover) < cfg.FailoverCooldown {
			return decision{kind: intentNone, message: "failover suppressed by cooldown"}, fc
		}
		sb, ok := pickStandby(pods, a.Name)
		if !ok {
			// Don't strip the only label: that would leave the Service with no
			// backend. Hold and let a standby recover.
			return decision{kind: intentHold,
				message: "active unhealthy but no healthy standby; holding current assignment"}, fc
		}
		fc[a.Name] = 0
		return decision{kind: intentFailover, target: sb.Name, current: a.Name,
			deleteCurrent:  cfg.DeleteStuckActive && a.Phase == "Running",
			recordFailover: true,
			message:        "active unhealthy; failing over to standby"}, fc
	}

	// ---- Zero actives: startup, or the active pod was just deleted. ----
	sb, ok := pickStandby(pods, "")
	if ok {
		return decision{kind: intentBootstrap, target: sb.Name,
			message: "no active pod; assigning one"}, fc
	}
	return decision{kind: intentHold, message: "no healthy pod available to mark active"}, fc
}

// healthy reports whether a pod is fit to keep serving as the active.
func healthy(p podState, cfg Config) bool {
	return p.Ready && p.Probe.OK && p.Probe.Latency <= cfg.ProbeLatencyBudget
}

// pickStandby returns the best promotion target: Ready, probe-OK, not deleting,
// not the excluded pod. Deterministic (lowest name) to avoid needless churn.
func pickStandby(pods []podState, exclude string) (podState, bool) {
	var cands []podState
	for _, p := range pods {
		if p.Name == exclude || p.Deleting {
			continue
		}
		if p.Ready && p.Probe.OK {
			cands = append(cands, p)
		}
	}
	if len(cands) == 0 {
		return podState{}, false
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].Name < cands[j].Name })
	return cands[0], true
}

// bestActive picks which active pod to keep when more than one is labelled:
// prefer a healthy one, then the lowest failure count, then the lowest name.
// If every candidate is unhealthy it still keeps one (the least-bad): demoting
// all of them would leave the Service with no backend, so we hold the assignment
// and let the normal unhealthy-active path handle the failover on a later pass.
func bestActive(actives []podState, fc map[string]int) podState {
	sort.Slice(actives, func(i, j int) bool {
		ai, aj := actives[i], actives[j]
		hi, hj := ai.Ready && ai.Probe.OK, aj.Ready && aj.Probe.OK
		if hi != hj {
			return hi // healthy first
		}
		if fc[ai.Name] != fc[aj.Name] {
			return fc[ai.Name] < fc[aj.Name]
		}
		return ai.Name < aj.Name
	})
	return actives[0]
}

// filterActive returns pods that currently carry the role label. A terminating
// pod is intentionally still counted as "active" here so the len==1 branch can
// detect it and promote a standby immediately; once the pod object is gone from
// the list the zero-active branch bootstraps a replacement.
func filterActive(pods []podState) []podState {
	var out []podState
	for _, p := range pods {
		if p.IsActive {
			out = append(out, p)
		}
	}
	return out
}

func cloneCounts(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func pruneCounts(fc map[string]int, pods []podState) {
	live := make(map[string]struct{}, len(pods))
	for _, p := range pods {
		live[p.Name] = struct{}{}
	}
	for name := range fc {
		if _, ok := live[name]; !ok {
			delete(fc, name)
		}
	}
}
