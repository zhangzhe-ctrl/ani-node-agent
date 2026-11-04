# ANI Node Agent — retired

ACC now derives current GPU allocation from Kubernetes Pods and Volcano assignments. It no longer accepts node telemetry, CRI/PodResources history or journal recovery. This repository is retained as historical source; the installable DaemonSet template has been removed. Do not deploy this agent with the current ACC release.

## Coordinated removal

The ACC schema 9 migration and replacement service must be deployed as one controlled cutover, with all old ACC writers stopped. After that cutover, remove the dedicated agent resources from each explicitly selected managed cluster:

| Resource | Namespace / identity |
|---|---|
| DaemonSet | `ani-node-system/ani-node-agent` |
| ServiceAccount | `ani-node-system/ani-node-agent` |
| Agent mTLS Secret | `ani-node-system/ani-node-agent-mtls` |
| Node selection label | `ani.io/node-agent-enabled` |
| ACC connection configuration | Remove the retired node-agent registration fields |
| Certificate provisioning | Stop issuing the agent-only `ani-node-agent` DNS identity |

Use the actual deployment inventory to identify task-specific resources before deletion. Remove the `ani-node-system` namespace only when it contains no other workloads or retained resources. Revoke any agent-only TokenReview grant on ACC; keep permissions needed by other workloads.

The host directory `/var/lib/ani-node-agent` is no longer read. Stop the agent before retiring that directory under the operator's retention policy. Do not remove the host journal, machine identity, container runtime sockets, kubelet PodResources socket, GPU drivers, NVIDIA runtime, Volcano plugin, HAMi monitor or physical GPU exporter: those are not agent-owned resources.

These are deployment instructions, not an executed cleanup. This change does not run cluster commands or remove host data. The next validation/deployment agent should record the selected cluster identities and actual removal results.

## Rollback boundary

Do not restart the old agent against the new ACC service: its RPC was removed. Rolling back ACC after schema 9 requires restoring the pre-migration database and matching old service versions under the coordinated database rollback procedure. The old deployment template remains available in Git history at commit `439c7897cb526ebbbc178443ae87e815eaa9d101`.
