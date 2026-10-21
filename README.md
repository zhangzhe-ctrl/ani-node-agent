# ANI Node Agent

This independent service collects local GPU inventory, PodResources, containerd CRI snapshots and runtime events. It sends typed `accelerator.node.v1.NodeTelemetryService/Connect` records to ACC. ACC owns all owner checks, allocation mapping, claims, bindings, budgets, initialization and release decisions. The Agent has no business database, owner key, tenant credential or inbound listener.

## Durable node state

The host state directory must survive Pod replacement. A nonblocking `flock` permits one writer. Before the first authenticated Welcome the directory stores only a stable provisional log ID. The actual Node UID from Welcome is fsynced into that identity before any fact can be appended. Existing identities must match the authenticated Node UID. No per-node UID list is required.

Each immutable protobuf record is deterministically encoded, hashed, written to its own file, fsynced and renamed with a directory fsync before becoming sendable. The on-disk envelope also checksums the exact bytes. Sequence numbers span all payload types. One Report is in flight; ACK sequence and digest must match it, and the ACK file plus directory are fsynced before the next Report is eligible. Only records below that durable ACK watermark are reclaimed.

On reconnect, Welcome can match the local ACK or the first unacknowledged record only. A larger watermark cannot discard a queue. ACK loss and process restart retransmit the original record bytes and original origins. Transport reconnect does not create a SourceGap. CRI subscription interruption, snapshot loss and local storage failure do.

The default unacknowledged storage limit is 512 MiB, with a preallocated 8 MiB reserve for failure markers and ACK writes. A full queue stops new sampling records while existing records can drain if ACK durability still works. A real gap is persisted before normal sampling resumes. Corrupt records, missing identity and impossible watermarks stop the process; they never reset the node history or silently discard unacknowledged data. A directory on a filesystem without the required preallocation and fsync behavior remains degraded.

## Manual installation

Build, formatting, dependency generation, tests, images and cluster operations run on Fedora. Local work is limited to editing, Git and transfer. The task deployment must first verify the permitted cluster identity and current state.

`deploy/kubernetes/daemonset.yaml` is an operator template. Before applying it, pin the image digest, ACC endpoint and exact server DNS SAN, the registered connection reference, and the actual unchanged ACC freshness. Provision the `ani-node-agent-mtls` Secret with a TLS 1.3 client certificate having exactly the DNS SAN `ani-node-agent`, its key, and the ACC trust CA. The center must register the same namespace, ServiceAccount, DaemonSet name and projected token audience. It verifies the real Pod, DaemonSet and Node via TokenReview and managed Kubernetes reads. There is no Agent Kubernetes Role or ClusterRole.

The template selects only operator-labelled nodes (`ani.io/node-agent-enabled=true`) and requires an already installed NVIDIA runtime class. Confirm this against the actual installation; this service does not install or switch runtimes, drivers or GPU plugins. The NVIDIA runtime injects only driver utility devices and libraries. No GPU workload allocation is requested. The runtime socket and PodResources directory must match the actual host paths. CRI requires runtime name `containerd`, API version `v1` and a nonempty diagnostic runtime version; the version alone does not establish source completeness. Read-only socket mounts do not make the runtime API read-only.

The Agent namespace has its own Pod Security setting because hostPath mounts cannot satisfy Baseline/Restricted. The Pod itself uses no privileged mode, host network, host PID or extra capabilities. Root is used for the actual host socket and journal permissions; verify the installed driver and runtime access before narrowing that UID. Existing `ani-system` policy is unaffected. The mounted paths are limited to the two source sockets, original journal, machine identity and Agent state directory. Remove journal/machine-id mounts and journal environment variables if that optional source is unavailable; journal recovery then returns unknown.

Snapshot and heartbeat default to 5 seconds, renewal to 10 seconds. The center lease is at most 30 seconds. `ANI_SOURCE_FRESHNESS` must match the existing center value and exceed the snapshot interval by more than 3 seconds; startup rejects an unsafe interval. `ANI_SNAPSHOT_INTERVAL` and `ANI_MAX_UNACKNOWLEDGED_BYTES` are explicit deployment overrides. A Report is limited to 8 MiB, with framing space reserved, and runtime event batches to 256 facts. Whitelisted GPU environment entries retain original order, duplicates and every memory index. Other environment variables and verbose CRI info remain local.

Each renewal rereads the projected token file. TLS certificate changes require a Pod restart; this preserves the log and Node UID while generating a new process instance ID. The host boot ID remains independent of Pod replacement.

Optional journal recovery uses the deployment-only `ANI_JOURNALCTL_PATH` (default `/usr/bin/journalctl`), which must be an absolute canonical path ending in `journalctl`. Recovery messages cannot supply commands or paths. The Agent checks the known attempts and the original sandbox/create/exit chain before reporting a positive result. ACC persists each issued scope before sending it, so a journal result can survive transport or center restart without losing its authorization. A result applies only to its requested Pod/container scope and never clears global source gaps.

## Recovery and verification

Preserve the state directory and all unacknowledged files during maintenance. For `LOCAL_LOG_ID_MISMATCH`, a Node UID replacement or corruption, stop the Agent and keep the original directory for investigation. Do not delete the log, reset sequence numbers or register a replacement log ID against existing center history. Reconcile the node identity and last receipt through the task recovery procedure before restarting. Storage faults recover only after durability is restored and a SourceGap has been appended. Heartbeats can report degradation; they do not refresh source observations or prove release.

On Fedora, select Go 1.26.10 and run `GOTOOLCHAIN=go1.26.10 GOWORK=off make verify` and `GOTOOLCHAIN=go1.26.10 GOWORK=off go build ./cmd/ani-node-agent`. State tests use the real filesystem and injectable fsync failure at the OS boundary. Source tests exercise typed CRI/PodResources data and the original bounded journal chain; they do not claim a live GPU or real center acceptance. Actual authenticated Agent → ACC/PG acceptance and hardware lifecycle evidence belong to the shared B/C result index.
