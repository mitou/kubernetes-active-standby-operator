# kubernetes-active-standby-operator

A tiny Kubernetes operator that keeps a workload in an **active/standby**
topology: several replicas run, but only **one** receives traffic at a time. The
operator health-probes the active pod and fails over to a healthy standby when
the active becomes unhealthy, stuck, or is terminated.

It is deliberately small — a single-replica Deployment that watches the managed
pods, runs a pure decision function, and patches one label. No CRDs, no
controller-runtime, no leader election.

## Why

Some applications misbehave when several replicas run behind a load balancer at
the same time without an explicit clustering/coordination mode — for example a
node-local search index that drifts between replicas, an in-memory cache that
goes stale, or per-node WebSocket state. Running such an app **active/standby**
(exactly one replica serving) sidesteps those problems while still keeping a warm
spare for fast failover.

## How it works

The front Service selects pods by a **role label** in addition to the app
label, e.g.:

```yaml
spec:
  selector:
    app: myapp
    active-standby-role: active   # <- only the active pod matches
```

The pod template carries only `app: myapp`. The operator adds
`active-standby-role: active` to exactly one healthy pod, so only that pod is an
endpoint of the Service (and therefore the only backend any load balancer in
front of it sees). On failure it moves the label to a healthy standby.

### Health model — never trust `phase: Running`

A pod can be `Running` yet wedged. Health is judged in layers:

1. **Pod readiness** (your app's own `readinessProbe`) decides Service/endpoint
   membership as usual. Point it at a *deep* health check so a stalled
   dependency flips the pod `NotReady`.
2. **Operator probe** — the operator independently HTTP-probes the *current
   active* pod (`POD_PORT` + `PROBE_PATH`) with a latency budget. A 2xx that is
   too slow, a non-2xx, or a missing `PROBE_EXPECT_BODY` substring counts as a
   failure. This catches a "Running but stuck/slow" active that still passes a
   shallow readiness check.
3. **Promotion safety** — only a pod that is `Ready` *and* passes the operator
   probe is eligible to be promoted.
4. **Recycle** — after demoting a stuck-but-`Running` active, the operator
   deletes it so its controller recreates a fresh standby.
5. **Anti-flap** — a healthy active is never moved (stickiness); demotion needs
   `FAILURE_THRESHOLD` consecutive failures; failovers are spaced by
   `FAILOVER_COOLDOWN`; if no pod is healthy the operator holds the last
   assignment and emits a `Degraded` Event rather than emptying the Service.

### Add-before-remove

On failover the operator **adds** the label to the new active, waits until that
pod appears as a *Ready* endpoint in the Service's `EndpointSlices`, and only
then **removes** the label from the old active. The Service therefore never has
zero backends during a switch. (We use EndpointSlices rather than a pod
readiness gate on purpose: gates like GKE's `load-balancer-neg-ready` are only
injected at pod-admission time for pods that already match a load-balanced
Service, so a standby never gets one.)

## Configuration

All configuration is via environment variables.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `NAMESPACE` / `POD_NAMESPACE` | ✓ | – | Namespace of the managed pods & Service |
| `POD_LABEL_SELECTOR` | ✓ | – | Label selector for the managed pods, e.g. `app=myapp` |
| `SERVICE_NAME` | ✓ | – | Service whose EndpointSlices front the pods |
| `ROLE_LABEL_KEY` | | `active-standby-role` | Label key the operator toggles |
| `ROLE_LABEL_VALUE` | | `active` | Label value marking the active pod |
| `POD_PORT` | | `8080` | Port the operator probe targets |
| `PROBE_PATH` | | `/healthz` | HTTP path of the operator probe |
| `PROBE_EXPECT_BODY` | | (none) | If set, probe body must contain this substring |
| `PROBE_TIMEOUT` | | `3s` | Per-probe timeout |
| `PROBE_LATENCY_BUDGET` | | `2s` | A 2xx slower than this is a soft failure |
| `RECONCILE_INTERVAL` | | `5s` | Reconcile tick (also event-driven on pod changes) |
| `FAILURE_THRESHOLD` | | `3` | Consecutive probe failures before failover |
| `FAILOVER_COOLDOWN` | | `60s` | Minimum time between failovers |
| `DELETE_STUCK_ACTIVE` | | `true` | Delete a demoted stuck-but-Running active |
| `ENDPOINT_PROGRAM_WAIT` | | `30s` | Max wait for the new endpoint before demoting the old |
| `POD_NAME` | | (none) | Operator's own pod name (downward API), for Events |

## Deploy

1. Your workload runs **≥ 2 replicas**, pod template labelled `app=myapp` (do
   **not** put the role label in the template — it would break the Deployment
   selector and the operator manages it at runtime).
2. The front Service selector includes the role label (`active-standby-role:
   active`).
3. Apply the operator (edit the namespace and the `REQUIRED` env vars):

```sh
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/deployment.yaml
```

The operator runs in the **same namespace** as the workload (its RBAC is a
namespaced Role).

### ⚠️ First cutover ordering

Narrowing the Service selector to `active-standby-role: active` while **no** pod
yet carries that label leaves the Service with zero backends. Cut over safely:

1. Deploy the operator **first** (Service selector still broad / app-only).
2. Wait until one pod is labelled: `kubectl get pods --show-labels`.
3. **Then** narrow the Service selector.

Once a pod is labelled, subsequent deploys are safe (the operator maintains the
label through rolling updates via its terminating-active fast path).

## Limitations

- **Operator down** → traffic continues to the current active (the label
  persists); only *automated failover* pauses until the operator reschedules.
- **Load-balancer sync latency** — EndpointSlice readiness is the best portable
  signal, but an external LB (e.g. a GKE NEG) still takes a few seconds to
  program. Add-before-remove overlaps old and new so this is normally seamless;
  verify on your platform under a real failover.
- **Background work** — active/standby steers *traffic*. If your standby still
  runs background jobs/schedulers against shared state, that is out of scope;
  use your app's own coordination for that.
- **One namespace per operator instance.**

## Example

See [`examples/mattermost/`](examples/mattermost/) for a complete configuration
(deep ping, `database_status` body guard, `mm-role` label).

## Development

```sh
go vet ./...
go test ./...
go build ./...
```

The decision logic (`decide()` in `reconcile.go`) is a pure function and is
covered by table tests in `reconcile_test.go`; the HTTP probe is covered in
`probe_test.go`. Neither needs a cluster.

## License

MIT — see [LICENSE](LICENSE).
