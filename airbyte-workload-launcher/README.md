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