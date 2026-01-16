/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteStorageConfigDefaultTest {
  @Inject
  private lateinit var airbyteStorageConfig: AirbyteStorageConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(StorageType.MINIO, airbyteStorageConfig.type)
    assertEquals(DEFAULT_STORAGE_LOCATION, airbyteStorageConfig.bucket.log)
    assertEquals(DEFAULT_STORAGE_LOCATION, airbyteStorageConfig.bucket.state)
    assertEquals(DEFAULT_STORAGE_LOCATION, airbyteStorageConfig.bucket.auditLogging)
    assertEquals(DEFAULT_STORAGE_LOCATION, airbyteStorageConfig.bucket.workloadOutput)
    assertEquals(DEFAULT_STORAGE_LOCATION, airbyteStorageConfig.bucket.activityPayload)
    assertEquals(DEFAULT_STORAGE_REPLICATION_DUMP_LOCATION, airbyteStorageConfig.bucket.replicationDump)
    assertEquals("", airbyteStorageConfig.azure.connectionString)
    assertEquals("", airbyteStorageConfig.gcs.applicationCredentials)
    assertEquals(DEFAULT_STORAGE_LOCAL_ROOT, airbyteStorageConfig.local.root)
    assertEquals("", airbyteStorageConfig.minio.endpoint)
    assertEquals("", airbyteStorageConfig.minio.accessKey)
    assertEquals("", airbyteStorageConfig.minio.secretAccessKey)
    assertEquals("", airbyteStorageConfig.s3.region)
    assertEquals("", airbyteStorageConfig.s3.accessKey)
    assertEquals("", airbyteStorageConfig.s3.secretAccessKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-storage-azure.yml"])
internal class AirbyteStorageConfigAzureTest {
  @Inject
  private lateinit var airbyteStorageConfig: AirbyteStorageConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(StorageType.AZURE, airbyteStorageConfig.type)
    assertEquals("test-log", airbyteStorageConfig.bucket.log)
    assertEquals("test-state", airbyteStorageConfig.bucket.state)
    assertEquals("test-audit-logging", airbyteStorageConfig.bucket.auditLogging)
    assertEquals("test-workload-output", airbyteStorageConfig.bucket.workloadOutput)
    assertEquals("test-activity-payload", airbyteStorageConfig.bucket.activityPayload)
    assertEquals("test-replication-dump", airbyteStorageConfig.bucket.replicationDump)
    assertEquals("test-connection-string", airbyteStorageConfig.azure.connectionString)
  }
}

@MicronautTest(propertySources = ["classpath:application-storage-gcs.yml"])
internal class AirbyteStorageConfigGcsTest {
  @Inject
  private lateinit var airbyteStorageConfig: AirbyteStorageConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(StorageType.GCS, airbyteStorageConfig.type)
    assertEquals("test-log", airbyteStorageConfig.bucket.log)
    assertEquals("test-state", airbyteStorageConfig.bucket.state)
    assertEquals("test-audit-logging", airbyteStorageConfig.bucket.auditLogging)
    assertEquals("test-workload-output", airbyteStorageConfig.bucket.workloadOutput)
    assertEquals("test-activity-payload", airbyteStorageConfig.bucket.activityPayload)
    assertEquals("test-replication-dump", airbyteStorageConfig.bucket.replicationDump)
    assertEquals("test-credentials", airbyteStorageConfig.gcs.applicationCredentials)
  }
}

@MicronautTest(propertySources = ["classpath:application-storage-local.yml"])
internal class AirbyteStorageConfigLocalTest {
  @Inject
  private lateinit var airbyteStorageConfig: AirbyteStorageConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(StorageType.LOCAL, airbyteStorageConfig.type)
    assertEquals("test-log", airbyteStorageConfig.bucket.log)
    assertEquals("test-state", airbyteStorageConfig.bucket.state)
    assertEquals("test-audit-logging", airbyteStorageConfig.bucket.auditLogging)
    assertEquals("test-workload-output", airbyteStorageConfig.bucket.workloadOutput)
    assertEquals("test-activity-payload", airbyteStorageConfig.bucket.activityPayload)
    assertEquals("test-replication-dump", airbyteStorageConfig.bucket.replicationDump)
    assertEquals("/other", airbyteStorageConfig.local.root)
  }
}

@MicronautTest(propertySources = ["classpath:application-storage-minio.yml"])
internal class AirbyteStorageConfigMinioTest {
  @Inject
  private lateinit var airbyteStorageConfig: AirbyteStorageConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(StorageType.MINIO, airbyteStorageConfig.type)
    assertEquals("test-log", airbyteStorageConfig.bucket.log)
    assertEquals("test-state", airbyteStorageConfig.bucket.state)
    assertEquals("test-audit-logging", airbyteStorageConfig.bucket.auditLogging)
    assertEquals("test-workload-output", airbyteStorageConfig.bucket.workloadOutput)
    assertEquals("test-activity-payload", airbyteStorageConfig.bucket.activityPayload)
    assertEquals("test-replication-dump", airbyteStorageConfig.bucket.replicationDump)
    assertEquals("test-minio-endpoint", airbyteStorageConfig.minio.endpoint)
    assertEquals("test-aws-access-key", airbyteStorageConfig.minio.accessKey)
    assertEquals("test-aws-secret-access-key", airbyteStorageConfig.minio.secretAccessKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-storage-s3.yml"])
internal class AirbyteStorageConfigS3Test {
  @Inject
  private lateinit var airbyteStorageConfig: AirbyteStorageConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(StorageType.S3, airbyteStorageConfig.type)
    assertEquals("test-log", airbyteStorageConfig.bucket.log)
    assertEquals("test-state", airbyteStorageConfig.bucket.state)
    assertEquals("test-audit-logging", airbyteStorageConfig.bucket.auditLogging)
    assertEquals("test-workload-output", airbyteStorageConfig.bucket.workloadOutput)
    assertEquals("test-activity-payload", airbyteStorageConfig.bucket.activityPayload)
    assertEquals("test-replication-dump", airbyteStorageConfig.bucket.replicationDump)
    assertEquals("US-WEST-1", airbyteStorageConfig.s3.region)
    assertEquals("test-aws-access-key", airbyteStorageConfig.s3.accessKey)
    assertEquals("test-aws-secret-access-key", airbyteStorageConfig.s3.secretAccessKey)
  }
}
