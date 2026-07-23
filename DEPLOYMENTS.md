# OSS deployment guide for preview

## Deployment model

The deployment flow is driven from the gitops repository rather than from this repository directly:

1. Environment-specific Helm values live under [../gitops/env/<env>/app/helm/<env>-ctrl-plane/values.yaml](../gitops/env).
2. Kubernetes manifests for ingress, secrets, and related resources live under [../gitops/env/<env>/app/manifests/<env>-ctrl-plane](../gitops/env).
3. DNS records are created by Terragrunt under [../gitops/env/<env>/infra/network/load-balancer/<env>-ctrl-plane/dns-record/terragrunt.hcl](../gitops/env).
4. The control-plane cluster itself is defined under [../gitops/env/<env>/infra/cluster/ctrl-plane](../gitops/env).

In practice this means each environment is deployed by:

- applying the env-specific Helm values for the control plane,
- applying the manifests for the ingress / service wiring,
- and validating the host through the DNS URL below.

## Environment to DNS mapping

### Quick access table

| Environment | Access URL |
| --- | --- |
| ab-dev-1 | https://ab-dev-1.internal.airbyte.dev |
| ab-dev-2 | https://ab-dev-2.internal.airbyte.dev |
| ab-dev-3 | https://ab-dev-3.internal.airbyte.dev |
| data-team | https://data-team.internal.airbyte.dev |
| engineering-land | https://engineering-land.internal.airbyte.dev |
| frontend-dev | https://frontend-dev.internal.airbyte.dev |
| frontend-dev-cloud | https://frontend-dev-cloud.internal.airbyte.dev |
| frontend-dev-ent | https://frontend-dev-ent.internal.airbyte.dev |
| frontend-dev-oss | https://frontend-dev-oss.internal.airbyte.dev |
| frontend-dev-preview | https://*.frontend-dev-preview.internal.airbyte.dev |
| preview | https://preview.internal.airbyte.dev |
| prod | https://internal.airbyte.dev |
| stage | https://stage.internal.airbyte.dev |
| support | https://support.internal.airbyte.dev |

## Cluster layout and component placement

Each environment is typically split into two Kubernetes clusters:

- the control plane cluster, which hosts the public ingress, Airbyte UI, API/server processes, and the control-plane services that receive user traffic;
- the data plane cluster, which hosts workload execution such as job pods, connector runtime, and other worker components that actually run syncs and connections.

In the gitops repository this is reflected by separate control-plane and data-plane directories under each environment, for example [../gitops/env/ab-dev-1/app/manifests/ab-dev-1-ctrl-plane](../gitops/env/ab-dev-1/app/manifests/ab-dev-1-ctrl-plane) and [../gitops/env/ab-dev-1/app/manifests/ab-dev-1-data-plane-0](../gitops/env/ab-dev-1/app/manifests/ab-dev-1-data-plane-0). The control-plane cluster is the place where the public hostname and ingress live; the data-plane cluster is not exposed directly to users.

### How the control plane talks to the data plane

The control plane does not usually talk to the data plane over the public internet. Instead, it uses the internal network and cluster-to-cluster connectivity that is provisioned for the environment. In practice:

- the control plane schedules or triggers jobs for the data plane,
- the data plane receives execution requests over the internal network,
- and the control plane uses the data plane's worker endpoints and workload metadata to track progress and report results.

In other words, the relationship is operational rather than user-facing: the control plane is the entry point and coordinator, while the data plane is the execution environment.

## Simple architecture

```text
Users / Browser
      |
      v
Control plane cluster
  |-- Public ingress / load balancer
  |-- Airbyte UI
  |-- Airbyte API / server
  |-- Auth / secrets / config
      |
      +--> Data plane cluster
            |-- Job pods / workers
            |-- Connector runtime
            |-- Sync execution
            |-- Temporal / workload components
            |
            +--> External sources / destinations
```

## Quick traffic flow for preview.internal.airbyte.dev

```text
Browser / client
      |
      v
DNS -> 34.54.96.203
      |
      v
GCP external HTTPS load balancer
  |-- forwarding rule :443
  |-- HTTPS proxy + ManagedCertificate
  |-- FrontendConfig: HTTP -> HTTPS redirect
  |-- URL map: /api and / -> server backend
      |
      v
Cloud Armor policy "preview"
  |-- allow: 34.162.28.2 / 34.162.24.26 (tailnet/internal)
  |-- deny(404): everyone else
      |
      v
BackendConfig -> NodePort service
      |
      v
airbyte-airbyte-server-svc NodePort -> airbyte-server pod:8001
      |
      v
OIDC login via keycloak.internal.airbyte.dev
```

Key points:

- the network gate is Cloud Armor, not OIDC;
- off-tailnet traffic is stopped at the Cloud Armor layer and returns `404` before reaching the app;
- the app-level identity layer is OIDC, which is enforced after the request is allowed through the network path.

## How to verify an environment

For any environment above, verify the deployment with these checks:

- confirm the ingress object exists in the target cluster,
- confirm the DNS record is present in the corresponding load-balancer module,
- confirm the data-plane manifests are present in the matching environment tree,
- and confirm the host responds over HTTPS on the expected URL.

The most common entry points to inspect are the ingress manifest, the host-specific Helm values, and the matching data-plane manifests listed in the environment tree.

## Manual post-deployment steps for preview-style ingress wiring

After a deployment, the ingress and service wiring may need a small manual follow-up in the control-plane cluster. The commands below are the ones used for the preview environment:

```bash
kubectl -n ab patch svc airbyte-airbyte-server-svc -p '{"spec":{"type":"NodePort"}}'
kubectl -n ab annotate svc airbyte-airbyte-server-svc cloud.google.com/backend-config='{"default":"preview-backend-config"}' --overwrite
kubectl -n ab apply -f env/preview/app/manifests/preview-ctrl-plane/preview-airbyte-ingress.yaml
```

### Verify

```bash
kubectl -n ab get ingress preview-airbyte
kubectl -n ab describe managedcertificate preview.internal.airbyte.dev | grep -i -A3 status
gcloud compute backend-services list --project preview-1744035508 --format="table(name,securityPolicy)"
curl -sS -o /dev/null -w '%{http_code}\n' https://preview.internal.airbyte.dev/api/v1/health
```

Expected outcomes:

- the managed certificate reports an `Active` status,
- the backend service appears in the GCP backend-services list,
- and the health endpoint returns `200` over HTTPS.

If you test from off-tailnet on a phone, the hostname may still be blocked or return a non-success response depending on network access.
