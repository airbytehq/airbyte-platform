package io.airbyte.workers.storage

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import io.airbyte.config.storage.GcsStorageConfig
import io.airbyte.config.storage.LocalStorageConfig
import io.airbyte.config.storage.MinioStorageConfig
import io.airbyte.config.storage.S3StorageConfig
import io.airbyte.config.storage.StorageBucketConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.nio.charset.StandardCharsets
import java.nio.file.Path

private const val KEY = "a"
private const val DOC1 = "hello"
private const val DOC2 = "bye"

private val buckets = StorageBucketConfig(log = "log", state = "state", workloadOutput = "workload", activityPayload = "payload")

class DocumentTypeTest {
  @Test
  fun `prefixes are correct`() {
    assertEquals(DocumentType.LOGS.prefix, Path.of("/job-logging"))
    assertEquals(DocumentType.STATE.prefix, Path.of("/state"))
    assertEquals(DocumentType.WORKLOAD_OUTPUT.prefix, Path.of("/workload/output"))
  }
}

class GcsStorageClientTest {
  private val config = GcsStorageConfig(buckets = buckets, applicationCredentials = "app-creds")

  @Test
  fun `blobId matches`() {
    val gcsClient: Storage = mockk()

    val clientState = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)
    assertEquals(BlobId.of(buckets.state, "/state/$KEY"), clientState.blobId(KEY))

    val workloadState = GcsStorageClient(config = config, type = DocumentType.WORKLOAD_OUTPUT, gcsClient = gcsClient)
    assertEquals(BlobId.of(buckets.workloadOutput, "/workload/output/$KEY"), workloadState.blobId(KEY))
  }

  @Test
  fun `read missing doc`() {
    val gcsClient: Storage = mockk()
    val client = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)

    // verify no blob is returned
    every { gcsClient.get(client.blobId(KEY)) } returns null
    assertNull(client.read(KEY), "key $KEY should be null")

    // verify blob is returned by exists is false
    every { gcsClient.get(client.blobId(KEY)) } returns
      mockk<Blob> {
        every { exists() } returns false
      }
    assertNull(client.read(KEY), "key $KEY should be null")
  }

  @Test
  fun `read existing doc`() {
    val gcsClient: Storage = mockk()
    val client = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)

    val blobId = client.blobId(KEY)
    every { gcsClient.get(blobId) } returns
      mockk<Blob> {
        every { exists() } returns true
      }
    every { gcsClient.readAllBytes(blobId) } returns DOC1.toByteArray()

    with(client.read(KEY)) {
      assertNotNull(this, "key $KEY should not be null")
      assertEquals(DOC1, this)
    }
  }

  @Test
  fun `write doc`() {
    val gcsClient: Storage = mockk()
    val client = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)

    val blobId = client.blobId(KEY)
    every {
      gcsClient.create(BlobInfo.newBuilder(blobId).build(), DOC1.toByteArray())
    } returns mockk<Blob>()

    client.write(KEY, DOC1)
    verify { gcsClient.create(BlobInfo.newBuilder(blobId).build(), DOC1.toByteArray()) }
  }

  @Test
  fun `delete doc`() {
    val gcsClient: Storage = mockk()
    val client = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)

    val blobId = client.blobId(KEY)

    // doc not deleted
    every { gcsClient.delete(blobId) } returns false
    assertFalse(client.delete(KEY))

    // doc deleted
    every { gcsClient.delete(blobId) } returns true
    assertTrue(client.delete(KEY))
  }
}

class LocalStorageClientTest {
  @Test
  fun `happy path`(
    @TempDir tempDir: Path,
  ) {
    val config = LocalStorageConfig(buckets = buckets, root = tempDir.toString())
    val client = LocalStorageClient(config = config, type = DocumentType.STATE)

    with(client.read(KEY)) {
      assertNull(this, "key $KEY should not exist")
    }

    client.write(KEY, DOC1)
    with(client.read(KEY)) {
      assertNotNull(this, "key $KEY should exist")
      assertEquals(DOC1, this)
    }

    client.write(KEY, DOC2)
    with(client.read(KEY)) {
      assertNotNull(this, "key $KEY should exist")
      assertEquals(DOC2, this)
    }

    assertTrue(client.delete(KEY))
    assertFalse(client.delete(KEY))

    with(client.read(KEY)) {
      assertNull(this, "key $KEY should not exist")
    }
  }
}

class MinioStorageClientTest {
  private val config = MinioStorageConfig(buckets = buckets, accessKey = "access", secretAccessKey = "secret", endpoint = "endpoint")

  @Test
  fun `key matches`() {
    val s3Client: S3Client = mockk()

    val clientState = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)
    assertEquals("/state/$KEY", clientState.key(KEY))

    val workloadState = MinioStorageClient(config = config, type = DocumentType.WORKLOAD_OUTPUT, s3Client = s3Client)
    assertEquals("/workload/output/$KEY", workloadState.key(KEY))
  }

  @Test
  fun `read missing doc`() {
    val s3Client: S3Client = mockk()
    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      GetObjectRequest.builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()

    val exception = NoSuchKeyException.builder().build()

    // verify no doc is returned
    every { s3Client.getObjectAsBytes(request) } throws exception
    assertNull(client.read(KEY), "key $KEY should be null")
  }

  @Test
  fun `read existing doc`() {
    val s3Client: S3Client = mockk()
    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      GetObjectRequest.builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()

    every { s3Client.getObjectAsBytes(request) } returns
      mockk<ResponseBytes<GetObjectResponse>> {
        every { asString(StandardCharsets.UTF_8) } returns DOC1
      }

    with(client.read(KEY)) {
      assertNotNull(this, "key $KEY should not be null")
      assertEquals(DOC1, this)
    }
  }

  @Test
  fun `write doc`() {
    val s3Client: S3Client = mockk()
    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      PutObjectRequest.builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()

    // It appears that RequestBody.fromString(DOC1) does not equal RequestBody.fromString(DOC1), hence the any() usage here
    every { s3Client.putObject(request, any<RequestBody>()) } returns mockk()

    client.write(KEY, DOC1)
  }

  @Test
  fun `delete doc`() {
    val s3Client: S3Client = mockk()
    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val existsRequest = HeadObjectRequest.builder().bucket(buckets.state).key(client.key(KEY)).build()
    val deleteRequest = DeleteObjectRequest.builder().bucket(buckets.state).key(client.key(KEY)).build()
    val exception = NoSuchKeyException.builder().build()

    // doc does not exist
    every { s3Client.headObject(existsRequest) } throws exception
    every { s3Client.deleteObject(deleteRequest) } returns mockk()
    assertFalse(client.delete(KEY))

    // doc deleted
    every { s3Client.headObject(existsRequest) } returns mockk()
    every { s3Client.deleteObject(deleteRequest) } returns mockk()
    assertTrue(client.delete(KEY))
  }
}

class S3StorageClientTest {
  private val config = S3StorageConfig(buckets = buckets, accessKey = "access", secretAccessKey = "secret", region = "us-east-1")

  @Test
  fun `key matches`() {
    val s3Client: S3Client = mockk()

    val clientState = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)
    assertEquals("/state/$KEY", clientState.key(KEY))

    val workloadState = S3StorageClient(config = config, type = DocumentType.WORKLOAD_OUTPUT, s3Client = s3Client)
    assertEquals("/workload/output/$KEY", workloadState.key(KEY))
  }

  @Test
  fun `read missing doc`() {
    val s3Client: S3Client = mockk()
    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      GetObjectRequest.builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()

    val exception = NoSuchKeyException.builder().build()

    // verify no doc is returned
    every { s3Client.getObjectAsBytes(request) } throws exception
    assertNull(client.read(KEY), "key $KEY should be null")
  }

  @Test
  fun `read existing doc`() {
    val s3Client: S3Client = mockk()
    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      GetObjectRequest.builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()

    every { s3Client.getObjectAsBytes(request) } returns
      mockk<ResponseBytes<GetObjectResponse>> {
        every { asString(StandardCharsets.UTF_8) } returns DOC1
      }

    with(client.read(KEY)) {
      assertNotNull(this, "key $KEY should not be null")
      assertEquals(DOC1, this)
    }
  }

  @Test
  fun `write doc`() {
    val s3Client: S3Client = mockk()
    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      PutObjectRequest.builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()

    // It appears that RequestBody.fromString(DOC1) does not equal RequestBody.fromString(DOC1), hence the any() usage here
    every { s3Client.putObject(request, any<RequestBody>()) } returns mockk()

    client.write(KEY, DOC1)
  }

  @Test
  fun `delete doc`() {
    val s3Client: S3Client = mockk()
    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val existsRequest = HeadObjectRequest.builder().bucket(buckets.state).key(client.key(KEY)).build()
    val deleteRequest = DeleteObjectRequest.builder().bucket(buckets.state).key(client.key(KEY)).build()
    val exception = NoSuchKeyException.builder().build()

    // doc does not exist
    every { s3Client.headObject(existsRequest) } throws exception
    every { s3Client.deleteObject(deleteRequest) } returns mockk()
    assertFalse(client.delete(KEY))

    // doc deleted
    every { s3Client.headObject(existsRequest) } returns mockk()
    every { s3Client.deleteObject(deleteRequest) } returns mockk()
    assertTrue(client.delete(KEY))
  }
}
