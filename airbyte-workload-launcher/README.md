# For local dev

### Enabling in Helm

The workload launcher is enabled by default in the v2 chart. To toggle it, set
`workloadLauncher.enabled` in `oss/charts/v2/airbyte/values.yaml`.

### S3 Bucket Creation errors
If you are having issues with your bucket not being created locally add the following to the `S3DocumentStoreClient` constructor at line 70.

```java
    final boolean bucketExist = s3Client.listBuckets().buckets().stream().anyMatch(bucket -> bucket.name().equals(bucketName));
    if (!bucketExist) {
      try {
        s3Client.createBucket(CreateBucketRequest.builder()
            .bucket(bucketName)
            .build());
      } catch (final Exception e) {
        LOGGER.error("Failed to initialize bucket for doc store.", e);
        throw e;
      }
    }
```

## Fusion destination copying

For Fusion-enabled sync/reset workloads, the launcher passes
`AIRBYTE_ORGANIZATION_ID`, `AIRBYTE_WORKSPACE_ID`, `AIRBYTE_SOURCE_ID`,
`AIRBYTE_DESTINATION_ID`, and `AIRBYTE_CONNECTION_ID` to Fusion-enabled destination
connectors from the sync input. Reset pods only have a destination connector.
Check, discover, and spec are unchanged.

The launcher rollout is controlled separately from the organization-level
Config API/UI enablement. The LaunchDarkly flag
`platform.fusion-launch-enabled` defaults to `false`; target dataplane groups to
enable resolution gradually. This code change does not create or configure the
LaunchDarkly flag. The launcher's feature-flag context includes the current
dataplane and dataplane-group IDs. The Config API continues to enforce its
organization-level rollout and managed-region eligibility.
The Config API's `FUSION_AWS_ACCOUNT_ID` remains the authoritative account for
the writer role ARN. The launcher validates the ARN structure, 12-digit account,
and organization-specific writer-role name without maintaining a second account
setting. The Config API independently validates workload assignment, tenant,
managed-region eligibility, and both actor facts.
The Cloud server's `airbyte.fusion.enablement.bucket` must also be configured.
Connector images, versions, and custom-connector status do not restrict Fusion
eligibility. Latest connector versions are assumed to support copying; users are
responsible for compatibility when pinning older versions.

Immediately before creating a new pod, the launcher calls
`POST /api/v1/connections/resolve_enablement_for_sync` using its dataplane
service-account authentication. The Config API resolves the persisted flags:
copying requires `enable_indexing=true` on both the source and destination, and
indexing requires `enable_agent_access=true` on each actor. Eligible destinations receive
`AIRBYTE_FUSION_ENABLED` and the four required `AIRBYTE_FUSION_S3_*` settings plus, only in local development
against LocalStack, an optional `AIRBYTE_S3_COPY_ENDPOINT` http(s) URL that
overrides the S3/STS endpoint; a connector given this endpoint must use path-style
addressing. Any other key fails the launch. Destination `enable_backfill` and
nullable RFC3339 `backfill_start_time` do not control copying or add launcher environment
variables. Actor flags are configured through `/api/v1/sources/{sourceId}/enablement`
and `/api/v1/destinations/{destinationId}/enablement`; the launcher consumes the
resolver's decision rather than interpreting actor flags itself. The writer ARN is
`arn:aws:iam::<server-configured-account-id>:role/airbyte-fusion-writer-<organization_id>`.
Generic IDs are not repeated under the copy prefix. Read/authentication failures
fail the launch; they are not interpreted as disabled enablement. Because the
launcher cannot know whether an actor is Fusion-enabled until the resolver
returns, an outage can also fail launches for actors that would resolve as
disabled. Config API calls use the injected client's configured authentication,
timeout, and retry behavior.

Eligible destinations require `AWS_ASSUME_ROLE_SECRET_NAME` to name an existing
Kubernetes secret in the workload namespace, with keys `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. The launcher only emits required `secretKeyRef` entries;
it never reads or serializes the credential values. This reuses the existing
connector AWS bootstrap secret and deduplicates credentials when both mechanisms apply. The bootstrap principal must
be allowed to assume the organization-specific writer role. Secret provisioning,
rotation, role trust/policies and Helm bindings belong to a separate infrastructure
change. Provision them before activating the cohort.

Revocation affects the next new pod, not already running connectors.
Activate copying only after the Config API/UI synchronization rollout,
existing opt-in reconciliation, and connector-owner validation of sync/reset copy
behavior are complete. Then enable the LaunchDarkly flag for a small dataplane
group cohort and expand it as launch behavior is validated.
