# For local dev

### Enabling in Helm

You will need to enable the server in Helm.

To do that run:

Edit `oss/charts/airbyte-workload-launcher/values.yaml` setting at the top level `enabled: true`

Edit `oss/charts/airbyte/values.yaml` setting `enabled: true` under `workload-launcher:`

Edit `oss/charts/airbyte/Chart.yaml.test` adding the following block:
```yaml
  - condition: airbyte-workload-launcher.enabled
    name: workload-launcher
    repository: "file://../airbyte-workload-launcher"
    version: "*"
```

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