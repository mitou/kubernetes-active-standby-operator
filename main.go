// Command active-standby-operator keeps a Kubernetes workload in an
// active/standby topology. Several replicas run, but only the pod carrying the
// role label is selected by the front Service (and therefore by any load
// balancer behind it), so exactly one pod receives traffic at a time. The
// operator continuously health-probes the active pod and fails over to a healthy
// standby when the active becomes unhealthy, stuck, or is terminated.
//
// It is intentionally small: a single-replica Deployment that watches the
// managed pods, runs a pure decision function, and patches a single label. See
// README.md for the configuration and the health model.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// Reconciler holds the operator's runtime dependencies and mutable state.
type Reconciler struct {
	cs        kubernetes.Interface
	podLister listersv1.PodLister
	cfg       Config

	failCount    map[string]int // podName -> consecutive operator-probe failures
	lastFailover time.Time
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	cfg := loadConfig()
	if err := cfg.validate(); err != nil {
		log.Fatalf("configuration error: %v", err)
	}
	log.Printf("starting active-standby-operator: ns=%s selector=%q service=%s roleLabel=%s=%s",
		cfg.Namespace, cfg.PodLabelSelector, cfg.ServiceName, cfg.RoleLabelKey, cfg.RoleLabelValue)

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("failed to build in-cluster config: %v", err)
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("failed to build clientset: %v", err)
	}

	sel, err := labels.Parse(cfg.PodLabelSelector)
	if err != nil {
		log.Fatalf("invalid POD_LABEL_SELECTOR %q: %v", cfg.PodLabelSelector, err)
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		cs, 30*time.Second,
		informers.WithNamespace(cfg.Namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = sel.String()
		}),
	)
	podInformer := factory.Core().V1().Pods()

	r := &Reconciler{
		cs:        cs,
		podLister: podInformer.Lister(),
		cfg:       cfg,
		failCount: map[string]int{},
	}

	ctx, cancel := signalContext()
	defer cancel()

	// Wake the reconcile loop promptly on pod changes (e.g. the active pod being
	// deleted) instead of waiting for the next tick. Buffered so handlers never
	// block; coalesced because one pending signal is enough.
	trigger := make(chan struct{}, 1)
	notify := func(interface{}) {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
	if _, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o interface{}) { notify(o) },
		UpdateFunc: func(_, o interface{}) { notify(o) },
		DeleteFunc: func(o interface{}) { notify(o) },
	}); err != nil {
		log.Fatalf("failed to register informer handler: %v", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.Informer().HasSynced) {
		log.Fatal("failed to sync informer cache")
	}
	log.Print("informer cache synced")

	go serveHealth(ctx)

	ticker := time.NewTicker(cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Print("shutting down")
			return
		case <-ticker.C:
		case <-trigger:
		}
		if err := r.reconcile(ctx); err != nil {
			log.Printf("reconcile error: %v", err)
		}
	}
}

// reconcile gathers the current pod states, probes the relevant pods, runs the
// pure decision logic and executes the resulting action.
func (r *Reconciler) reconcile(ctx context.Context) error {
	pods, err := r.podLister.Pods(r.cfg.Namespace).List(labels.Everything())
	if err != nil {
		return err
	}

	states := make([]podState, 0, len(pods))
	for _, p := range pods {
		ps := toPodState(p, r.cfg)
		// Probe pods that can serve (Ready) and always probe the current active
		// so we can detect a "Running but stuck" active that lost readiness.
		if ps.IP != "" && (ps.Ready || ps.IsActive) {
			ps.Probe = probePod(ctx, ps.IP, r.cfg.PodPort, r.cfg.ProbePath, r.cfg.ProbeExpectBody, r.cfg.ProbeTimeout)
		}
		states = append(states, ps)
	}

	dec, newFC := decide(states, r.failCount, time.Now(), r.lastFailover, r.cfg)
	r.failCount = newFC
	r.execute(ctx, dec)
	return nil
}

// execute performs the Kubernetes API calls implied by a decision. Failover uses
// the mandatory add-before-remove ordering so the Service never momentarily
// loses its only backend.
func (r *Reconciler) execute(ctx context.Context, dec decision) {
	switch dec.kind {
	case intentNone:
		if dec.message != "" {
			r.logf("steady: %s", dec.message)
		}

	case intentBootstrap:
		if err := r.addActiveLabel(ctx, dec.target); err != nil {
			r.logf("bootstrap: failed to label %s: %v", dec.target, err)
			return
		}
		r.emit(ctx, dec.target, corev1.EventTypeNormal, "ActiveAssigned", dec.message)

	case intentResolveSplitBrain:
		for _, loser := range dec.losers {
			if err := r.removeActiveLabel(ctx, loser); err != nil {
				r.logf("split-brain: failed to demote %s: %v", loser, err)
			}
		}
		r.emit(ctx, dec.target, corev1.EventTypeWarning, "SplitBrainResolved", dec.message)

	case intentFailover:
		// 1. promote the standby (now briefly two-active, but never zero).
		if err := r.addActiveLabel(ctx, dec.target); err != nil {
			r.logf("failover: failed to promote %s: %v", dec.target, err)
			return
		}
		// 2. wait until the new active can actually serve.
		r.waitEndpointReady(ctx, dec.target)
		// 3. demote the old active.
		if err := r.removeActiveLabel(ctx, dec.current); err != nil {
			r.logf("failover: failed to demote %s: %v", dec.current, err)
		}
		// 4. recycle a wedged-but-Running old active into a fresh standby.
		if dec.deleteCurrent {
			if err := r.deletePod(ctx, dec.current); err != nil {
				r.logf("failover: failed to delete stuck pod %s: %v", dec.current, err)
			}
		}
		// Only a genuine health-driven failover arms the cooldown; a promotion
		// caused by a terminating active (rolling update) must not suppress a
		// subsequent real failover.
		if dec.recordFailover {
			r.lastFailover = time.Now()
		}
		r.emit(ctx, dec.target, corev1.EventTypeWarning, "Failover", dec.message+
			" ("+dec.current+" -> "+dec.target+")")

	case intentHold:
		r.emit(ctx, "", corev1.EventTypeWarning, "Degraded", dec.message)
	}
}

func (r *Reconciler) logf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

// toPodState flattens a *corev1.Pod into the view the decision logic needs.
func toPodState(p *corev1.Pod, cfg Config) podState {
	ready := false
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			ready = true
			break
		}
	}
	return podState{
		Name:     p.Name,
		IP:       p.Status.PodIP,
		Ready:    ready,
		Phase:    string(p.Status.Phase),
		IsActive: p.Labels[cfg.RoleLabelKey] == cfg.RoleLabelValue,
		Deleting: p.DeletionTimestamp != nil,
	}
}

// serveHealth exposes liveness/readiness endpoints for the operator's own probes.
func serveHealth(ctx context.Context) {
	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok)
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("health server error: %v", err)
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	var once sync.Once
	go func() {
		<-ch
		once.Do(cancel)
	}()
	return ctx, cancel
}
