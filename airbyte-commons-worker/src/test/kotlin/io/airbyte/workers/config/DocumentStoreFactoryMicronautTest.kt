package io.airbyte.workers.config

import io.airbyte.config.storage.CloudStorageConfigs
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import javax.inject.Inject

@MicronautTest(environments = ["docstoreconfig"])
class DocumentStoreFactoryMicronautTest {
  @Inject
  lateinit var documentStoreConfigFactory: DocumentStoreConfigFactory

  @Test
  fun `it should load a gcs config`() {
    val config = documentStoreConfigFactory.get(DocumentType.STATE, CloudStorageConfigs.WorkerStorageType.GCS)
    assertEquals("gcs-bucket", config.gcsConfig.bucketName)
    assertEquals("gcs-app-creds", config.gcsConfig.googleApplicationCredentials)
  }

  @Test
  fun `it should load a local config`() {
    val config = documentStoreConfigFactory.get(DocumentType.STATE, CloudStorageConfigs.WorkerStorageType.LOCAL)
    assertEquals("local-root", config.localConfig.root)
  }

  @Test
  fun `it should load a minio config`() {
    val config = documentStoreConfigFactory.get(DocumentType.STATE, CloudStorageConfigs.WorkerStorageType.MINIO)
    assertEquals("minio-bucket", config.minioConfig.bucketName)
    assertEquals("minio-access-key", config.minioConfig.awsAccessKey)
    assertEquals("minio-secret", config.minioConfig.awsSecretAccessKey)
    assertEquals("minio-endpoint", config.minioConfig.minioEndpoint)
  }

  @Test
  fun `it should load an s3 config`() {
    val config = documentStoreConfigFactory.get(DocumentType.STATE, CloudStorageConfigs.WorkerStorageType.S3)
    assertEquals("s3-bucket", config.s3Config.bucketName)
    assertEquals("s3-key", config.s3Config.awsAccessKey)
    assertEquals("s3-secret", config.s3Config.awsSecretAccessKey)
    assertEquals("s3-region", config.s3Config.region)
  }
}
