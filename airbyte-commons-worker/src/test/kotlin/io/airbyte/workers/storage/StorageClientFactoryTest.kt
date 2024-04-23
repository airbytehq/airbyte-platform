package io.airbyte.workers.storage

import com.google.cloud.storage.Storage
import io.airbyte.config.storage.GcsStorageConfig
import io.airbyte.config.storage.LocalStorageConfig
import io.airbyte.config.storage.MinioStorageConfig
import io.airbyte.config.storage.S3StorageConfig
import io.airbyte.config.storage.STORAGE_TYPE
import io.airbyte.config.storage.StorageBucketConfig
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.s3.S3Client

/**
 * Note @MockBean doesn't work in this class for some reason, possible due to a Micronaut 3 problem.
 * When upgrading to Micronaut 4, the `@get:Primary` and `@get:Bean` annotations might be replaceable with @MockBean.
 */

private val bucket = StorageBucketConfig(log = "log", state = "state", workloadOutput = "workload", activityPayload = "payload")

@MicronautTest
@Property(name = STORAGE_TYPE, value = "local")
class LocalStorageClientFactoryTest {
  @Inject
  lateinit var factory: StorageClientFactory

  /**
   * Your IDE is going to say this property isn't used, ignore this as it is being used.
   * It is injected via Micronaut into the [StorageClientFactory] which is under test.
   */
  @get:Primary
  @get:Bean
  val localStorageConfig: LocalStorageConfig =
    mockk {
      every { root } returns "/tmp/test"
    }

  @Test
  fun `get returns correct class`() {
    val state: LocalStorageClient = factory.get(DocumentType.STATE) as LocalStorageClient
    assertEquals("/tmp/test/state", state.root.toString())

    val workload: LocalStorageClient = factory.get(DocumentType.WORKLOAD_OUTPUT) as LocalStorageClient
    assertEquals("/tmp/test/workload/output", workload.root.toString())

    val log: LocalStorageClient = factory.get(DocumentType.LOGS) as LocalStorageClient
    assertEquals("/tmp/test/job-logging", log.root.toString())
  }
}

@MicronautTest
@Property(name = STORAGE_TYPE, value = "gcs")
class GcsStorageClientFactoryTest {
  @get:Primary
  @get:Bean
  val storageConfig: GcsStorageConfig =
    mockk {
      every { buckets } returns bucket
      every { applicationCredentials } returns "mock-app-creds"
    }

  val gcsClient: Storage = mockk()

  init {
    mockkStatic(GcsStorageConfig::gcsClient)
    every { storageConfig.gcsClient() } returns gcsClient
  }

  @Inject
  lateinit var factory: StorageClientFactory

  @Test
  fun `get returns correct class`() {
    assertTrue(factory.get(DocumentType.LOGS) is GcsStorageClient, "log returned wrong type")
    assertTrue(factory.get(DocumentType.STATE) is GcsStorageClient, "state returned wrong type")
    assertTrue(factory.get(DocumentType.WORKLOAD_OUTPUT) is GcsStorageClient, "workload returned wrong type")
  }
}

@MicronautTest
@Property(name = STORAGE_TYPE, value = "minio")
class MinioStorageClientFactoryTest {
  @get:Primary
  @get:Bean
  val storageConfig: MinioStorageConfig =
    mockk {
      every { buckets } returns bucket
      every { accessKey } returns "mock-access"
      every { secretAccessKey } returns "mock-secret"
      every { endpoint } returns "mock-endpoint"
    }

  val s3Client: S3Client = mockk()

  init {
    mockkStatic(MinioStorageConfig::s3Client)
    every { storageConfig.s3Client() } returns s3Client
  }

  @Inject
  lateinit var factory: StorageClientFactory

  @Test
  fun `get returns correct class`() {
    assertTrue(factory.get(DocumentType.LOGS) is MinioStorageClient, "log returned wrong type")
    assertTrue(factory.get(DocumentType.STATE) is MinioStorageClient, "state returned wrong type")
    assertTrue(factory.get(DocumentType.WORKLOAD_OUTPUT) is MinioStorageClient, "workload returned wrong type")
  }
}

@MicronautTest
@Property(name = STORAGE_TYPE, value = "s3")
class S3StorageClientFactoryTest {
  @get:Primary
  @get:Bean
  val storageConfig: S3StorageConfig =
    mockk {
      every { buckets } returns bucket
      every { accessKey } returns "mock-access"
      every { secretAccessKey } returns "mock-secret"
      every { region } returns "mock-region"
    }

  val s3Client: S3Client = mockk()

  init {
    mockkStatic(S3StorageConfig::s3Client)
    every { storageConfig.s3Client() } returns s3Client
  }

  @Inject
  lateinit var factory: StorageClientFactory

  @Test
  fun `get returns correct class`() {
    assertTrue(factory.get(DocumentType.LOGS) is S3StorageClient, "log returned wrong type")
    assertTrue(factory.get(DocumentType.STATE) is S3StorageClient, "state returned wrong type")
    assertTrue(factory.get(DocumentType.WORKLOAD_OUTPUT) is S3StorageClient, "workload returned wrong type")
  }
}
