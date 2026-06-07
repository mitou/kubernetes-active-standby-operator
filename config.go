package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all tunables for the operator. Everything is read from the
// environment (12-factor) so the Deployment manifest can configure behaviour
// without code changes. The three identifiers that describe *which* workload to
// manage (Namespace, PodLabelSelector, ServiceName) are required; everything
// else has a sensible default.
type Config struct {
	Namespace        string // namespace the managed pods and Service live in
	PodLabelSelector string // selects the managed pods, e.g. app=myapp
	RoleLabelKey     string // the label the operator toggles, e.g. active-standby-role
	RoleLabelValue   string // the value that marks the active pod, e.g. active
	ServiceName      string // the Service whose EndpointSlices front the load balancer

	PodPort         int    // port the health probe targets on each pod
	ProbePath       string // HTTP path of the health probe
	ProbeExpectBody string // optional substring the probe body must contain
	ProbeTimeout    time.Duration
	// A 2xx response slower than this counts as a soft failure (stuck/slow pod).
	ProbeLatencyBudget time.Duration

	ReconcileInterval time.Duration
	// Consecutive operator-probe failures before the active pod is demoted.
	FailureThreshold int
	// Minimum time between two failovers (anti-flap).
	FailoverCooldown time.Duration
	// When true, a demoted "Running but stuck" pod is deleted so its controller
	// (Deployment/StatefulSet) recreates a fresh standby.
	DeleteStuckActive bool
	// Max time to wait for a freshly-promoted pod's endpoint to become Ready
	// before removing the label from the old active (add-before-remove).
	EndpointProgramWait time.Duration

	// SelfPodName is this operator pod's own name (downward API), used as the
	// involvedObject for Events that are not tied to a specific managed pod.
	SelfPodName string
}

func loadConfig() Config {
	return Config{
		Namespace:           firstNonEmpty(os.Getenv("NAMESPACE"), os.Getenv("POD_NAMESPACE")),
		PodLabelSelector:    os.Getenv("POD_LABEL_SELECTOR"),
		ServiceName:         os.Getenv("SERVICE_NAME"),
		RoleLabelKey:        getenvStr("ROLE_LABEL_KEY", "active-standby-role"),
		RoleLabelValue:      getenvStr("ROLE_LABEL_VALUE", "active"),
		PodPort:             getenvInt("POD_PORT", 8080),
		ProbePath:           getenvStr("PROBE_PATH", "/healthz"),
		ProbeExpectBody:     os.Getenv("PROBE_EXPECT_BODY"),
		ProbeTimeout:        getenvDur("PROBE_TIMEOUT", 3*time.Second),
		ProbeLatencyBudget:  getenvDur("PROBE_LATENCY_BUDGET", 2*time.Second),
		ReconcileInterval:   getenvDur("RECONCILE_INTERVAL", 5*time.Second),
		FailureThreshold:    getenvInt("FAILURE_THRESHOLD", 3),
		FailoverCooldown:    getenvDur("FAILOVER_COOLDOWN", 60*time.Second),
		DeleteStuckActive:   getenvBool("DELETE_STUCK_ACTIVE", true),
		EndpointProgramWait: getenvDur("ENDPOINT_PROGRAM_WAIT", 30*time.Second),
		SelfPodName:         os.Getenv("POD_NAME"),
	}
}

// validate reports an error if any required identifier is missing.
func (c Config) validate() error {
	var missing []string
	if c.Namespace == "" {
		missing = append(missing, "NAMESPACE (or POD_NAMESPACE)")
	}
	if c.PodLabelSelector == "" {
		missing = append(missing, "POD_LABEL_SELECTOR")
	}
	if c.ServiceName == "" {
		missing = append(missing, "SERVICE_NAME")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required configuration: %s", strings.Join(missing, ", "))
	}
	return nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func getenvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("invalid int for %s=%q, using default %d: %v", key, v, def, err)
			return def
		}
		return n
	}
	return def
}

func getenvDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Printf("invalid duration for %s=%q, using default %s: %v", key, v, def, err)
			return def
		}
		return d
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			log.Printf("invalid bool for %s=%q, using default %t: %v", key, v, def, err)
			return def
		}
		return b
	}
	return def
}
