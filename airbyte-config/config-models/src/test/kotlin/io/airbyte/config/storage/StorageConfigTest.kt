package io.airbyte.config.storage

import io.airbyte.commons.envvar.EnvVar
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

private const val VAL_BUCKET_LOG = "log"
private const val VAL_BUCKET_STATE = "state"
private const val VAL_BUCKET_WORKLOAD = "workload"
private const val VAL_BUCKET_ACTIVITY_PAYLOAD = "payload"

@MicronautTest(environments = ["storage-local"])
class LocalStorageConfigTest {
  @Inject
  lateinit var storageConfig: StorageConfig

  @Test
  fun `local type injected`() {
    assertTrue(storageConfig is LocalStorageConfig, "type is ${storageConfig::class.java}")
    with(storageConfig as LocalStorageConfig) {
      assertEquals(VAL_BUCKET_LOG, buckets.log)
      assertEquals(VAL_BUCKET_STATE, buckets.state)
      assertEquals(VAL_BUCKET_WORKLOAD, buckets.workloadOutput)
      assertEquals(VAL_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      assertEquals("/tmp", root)
    }
  }

  @Test
  fun `environment variables`() {
    with(storageConfig as LocalStorageConfig) {
      val expected: Map<String, String> =
        mapOf(
          EnvVar.STORAGE_TYPE to StorageType.LOCAL.name,
          EnvVar.STORAGE_BUCKET_LOG to VAL_BUCKET_LOG,
          EnvVar.STORAGE_BUCKET_STATE to VAL_BUCKET_STATE,
          EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT to VAL_BUCKET_WORKLOAD,
          EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD to VAL_BUCKET_ACTIVITY_PAYLOAD,
          EnvVar.LOCAL_ROOT to "/tmp",
        ).mapKeys { it.key.name }

      assertEquals(expected, toEnvVarMap())
    }
  }
}

@MicronautTest(environments = ["storage-gcs"])
class GcsStorageConfigTest {
  @Inject
  lateinit var storageConfig: StorageConfig

  @Test
  fun `correct type injected`() {
    assertTrue(storageConfig is GcsStorageConfig, "type is ${storageConfig::class.java}")
    with(storageConfig as GcsStorageConfig) {
      assertEquals(VAL_BUCKET_LOG, buckets.log)
      assertEquals(VAL_BUCKET_STATE, buckets.state)
      assertEquals(VAL_BUCKET_WORKLOAD, buckets.workloadOutput)
      assertEquals(VAL_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      assertEquals("credz", applicationCredentials)
    }
  }

  @Test
  fun `environment variables`() {
    with(storageConfig as GcsStorageConfig) {
      val expected: Map<String, String> =
        mapOf(
          EnvVar.STORAGE_TYPE to StorageType.GCS.name,
          EnvVar.STORAGE_BUCKET_LOG to VAL_BUCKET_LOG,
          EnvVar.STORAGE_BUCKET_STATE to VAL_BUCKET_STATE,
          EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT to VAL_BUCKET_WORKLOAD,
          EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD to VAL_BUCKET_ACTIVITY_PAYLOAD,
          EnvVar.GOOGLE_APPLICATION_CREDENTIALS to "credz",
        ).mapKeys { it.key.name }

      assertEquals(expected, toEnvVarMap())
    }
  }
}

@MicronautTest(environments = ["storage-minio"])
class MinioStorageConfigTest {
  @Inject
  lateinit var storageConfig: StorageConfig

  @Test
  fun `correct type injected`() {
    assertTrue(storageConfig is MinioStorageConfig, "type is ${storageConfig::class.java}")
    with(storageConfig as MinioStorageConfig) {
      assertEquals(VAL_BUCKET_LOG, buckets.log)
      assertEquals(VAL_BUCKET_STATE, buckets.state)
      assertEquals(VAL_BUCKET_WORKLOAD, buckets.workloadOutput)
      assertEquals(VAL_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      assertEquals("access", this.accessKey)
      assertEquals("secret-access", secretAccessKey)
      assertEquals("endpoint", endpoint)
    }
  }

  @Test
  fun `environment variables`() {
    with(storageConfig as MinioStorageConfig) {
      val expected: Map<String, String> =
        mapOf(
          EnvVar.STORAGE_TYPE to StorageType.MINIO.name,
          EnvVar.STORAGE_BUCKET_LOG to VAL_BUCKET_LOG,
          EnvVar.STORAGE_BUCKET_STATE to VAL_BUCKET_STATE,
          EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT to VAL_BUCKET_WORKLOAD,
          EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD to VAL_BUCKET_ACTIVITY_PAYLOAD,
          EnvVar.AWS_ACCESS_KEY_ID to "access",
          EnvVar.AWS_SECRET_ACCESS_KEY to "secret-access",
          EnvVar.MINIO_ENDPOINT to "endpoint",
        ).mapKeys { it.key.name }

      assertEquals(expected, toEnvVarMap())
    }
  }
}

@MicronautTest(environments = ["storage-s3"])
class S3StorageConfigTest {
  @Inject
  lateinit var storageConfig: StorageConfig

  @Test
  fun `correct type injected`() {
    assertTrue(storageConfig is S3StorageConfig, "type is ${storageConfig::class.java}")
    with(storageConfig as S3StorageConfig) {
      assertEquals(VAL_BUCKET_LOG, buckets.log)
      assertEquals(VAL_BUCKET_STATE, buckets.state)
      assertEquals(VAL_BUCKET_WORKLOAD, buckets.workloadOutput)
      assertEquals(VAL_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      assertEquals("access", this.accessKey)
      assertEquals("secret-access", secretAccessKey)
      assertEquals("us-moon-1", region)
    }
  }

  @Test
  fun `environment variables`() {
    with(storageConfig as S3StorageConfig) {
      val expected: Map<String, String> =
        mapOf(
          EnvVar.STORAGE_TYPE to StorageType.S3.name,
          EnvVar.STORAGE_BUCKET_LOG to VAL_BUCKET_LOG,
          EnvVar.STORAGE_BUCKET_STATE to VAL_BUCKET_STATE,
          EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT to VAL_BUCKET_WORKLOAD,
          EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD to VAL_BUCKET_ACTIVITY_PAYLOAD,
          EnvVar.AWS_ACCESS_KEY_ID to "access",
          EnvVar.AWS_SECRET_ACCESS_KEY to "secret-access",
          EnvVar.AWS_DEFAULT_REGION to "us-moon-1",
        ).mapKeys { it.key.name }

      assertEquals(expected, toEnvVarMap())
    }
  }
}

class StorageConfigTest {
  @Test
  fun `verify builders do not log private information`() {
    val private = "nobody"
    val public = "everybody"
    val masked = "*******"
    val buckets = StorageBucketConfig(log = "log", state = "state", workloadOutput = "workload", activityPayload = "payload")

    val gcs = GcsStorageConfig(buckets = buckets, applicationCredentials = private)
    val minio =
      MinioStorageConfig(
        buckets = buckets,
        accessKey = private,
        secretAccessKey = private,
        endpoint = public,
      )
    val s3 =
      S3StorageConfig(
        buckets = buckets,
        accessKey = private,
        secretAccessKey = private,
        region = public,
      )

    with(gcs.toString()) {
      assertFalse(contains(private))
      assertTrue(contains(masked))
    }
    with(minio.toString()) {
      assertFalse(contains(private))
      assertTrue(contains(masked))
    }
    with(s3.toString()) {
      assertFalse(contains(private))
      assertTrue(contains(masked))
    }
  }
}
