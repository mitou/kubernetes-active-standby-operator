package main

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// addActiveLabel attaches the role label to a pod via a strategic-merge patch.
// Patching a single key never clobbers other labels and keeps the op idempotent.
func (r *Reconciler) addActiveLabel(ctx context.Context, podName string) error {
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, r.cfg.RoleLabelKey, r.cfg.RoleLabelValue)
	_, err := r.cs.CoreV1().Pods(r.cfg.Namespace).Patch(
		ctx, podName, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// removeActiveLabel deletes the role label. In a strategic-merge patch a null
// value removes the map key.
func (r *Reconciler) removeActiveLabel(ctx context.Context, podName string) error {
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:null}}}`, r.cfg.RoleLabelKey)
	_, err := r.cs.CoreV1().Pods(r.cfg.Namespace).Patch(
		ctx, podName, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// deletePod gracefully deletes a pod so its controller recreates a fresh
// standby. We use the delete API (not eviction) deliberately: it bypasses any
// PodDisruptionBudget, which is what we want for forced recovery of a wedged pod
// — and it is only ever called after a healthy standby has already become active.
func (r *Reconciler) deletePod(ctx context.Context, podName string) error {
	return r.cs.CoreV1().Pods(r.cfg.Namespace).Delete(ctx, podName, metav1.DeleteOptions{})
}

// waitEndpointReady blocks until the freshly-promoted pod appears as a Ready
// endpoint in the Service's EndpointSlices, or until the timeout elapses. This
// is the gap the add-before-remove ordering protects: we only drop the old
// active once the new one is actually programmed into the Service (and thus the
// load balancer behind it).
//
// We deliberately do NOT rely on a pod readiness gate (such as GKE's
// cloud.google.com/load-balancer-neg-ready) here: such gates are only injected
// at pod-admission time for pods that already match a load-balanced Service
// selector, so a standby — which carries no role label until we promote it —
// never receives the gate, making a gate check a no-op. The Service's
// EndpointSlice is the reliable, portable signal.
func (r *Reconciler) waitEndpointReady(ctx context.Context, podName string) {
	deadline := time.Now().Add(r.cfg.EndpointProgramWait)
	for {
		ready, err := r.endpointReady(ctx, podName)
		if err != nil {
			r.logf("waitEndpointReady: error listing endpointslices: %v", err)
		} else if ready {
			return
		}
		if time.Now().After(deadline) {
			r.logf("waitEndpointReady: timed out after %s waiting for %s to appear as a ready endpoint; proceeding",
				r.cfg.EndpointProgramWait, podName)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// endpointReady reports whether podName is present as a Ready endpoint in any of
// the EndpointSlices that back the configured Service.
func (r *Reconciler) endpointReady(ctx context.Context, podName string) (bool, error) {
	slices, err := r.cs.DiscoveryV1().EndpointSlices(r.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: discoveryv1.LabelServiceName + "=" + r.cfg.ServiceName,
	})
	if err != nil {
		return false, err
	}
	for _, s := range slices.Items {
		for _, ep := range s.Endpoints {
			if ep.TargetRef == nil || ep.TargetRef.Name != podName {
				continue
			}
			if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
				return true, nil
			}
		}
	}
	return false, nil
}

// emit records a Kubernetes Event and logs it. Events are how the operator
// surfaces failovers and degraded states to whatever monitoring is watching the
// namespace, without needing any cloud credentials. involvedPod may be empty, in
// which case the operator's own pod is referenced.
func (r *Reconciler) emit(ctx context.Context, involvedPod, eventType, reason, message string) {
	r.logf("%s/%s: %s", eventType, reason, message)

	name := involvedPod
	if name == "" {
		name = r.cfg.SelfPodName
	}
	if name == "" {
		return // nothing sensible to attach the Event to
	}

	now := metav1.Now()
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "active-standby-operator-",
			Namespace:    r.cfg.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: r.cfg.Namespace,
			Name:      name,
		},
		Reason:         reason,
		Message:        message,
		Type:           eventType,
		Source:         corev1.EventSource{Component: "active-standby-operator"},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
	}
	if _, err := r.cs.CoreV1().Events(r.cfg.Namespace).Create(ctx, ev, metav1.CreateOptions{}); err != nil {
		r.logf("failed to create event: %v", err)
	}
}
