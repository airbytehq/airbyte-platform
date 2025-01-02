/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import com.azure.core.util.BinaryData
import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.BlobItem
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import io.airbyte.commons.envvar.EnvVar
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
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.CreateBucketResponse
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files.createDirectory
import java.nio.file.Path
import kotlin.io.path.createTempDirectory
import kotlin.io.path.createTempFile
import kotlin.io.path.exists
import kotlin.io.path.pathString
import com.google.cloud.storage.Bucket as GcsBucket

private const val KEY = "a"
private const val DOC1 = "hello"
private const val DOC2 = "bye"

private val buckets =
  StorageBucketConfig(
    log = "log",
    state = "state",
    workloadOutput = "workload",
    activityPayload = "payload",
    auditLogging = null,
  )

internal class DocumentTypeTest {
  @Test
  fun `prefixes are correct`() {
    assertEquals(DocumentType.LOGS.prefix, Path.of("job-logging"))
    assertEquals(DocumentType.STATE.prefix, Path.of("/state"))
    assertEquals(DocumentType.WORKLOAD_OUTPUT.prefix, Path.of("/workload/output"))
    assertEquals(DocumentType.AUDIT_LOGS.prefix, Path.of("audit-logging"))
  }
}

internal class AzureStorageClientTest {
  private val config = AzureStorageConfig(buckets = buckets, connectionString = "connect")

  @Test
  fun `key matches`() {
    val azureClient: BlobServiceClient = mockk()
    val blobContainerClient: BlobContainerClient = mockk()

    every { azureClient.getBlobContainerClient(any()) } returns blobContainerClient
    every { blobContainerClient.exists() } returns false
    every { blobContainerClient.createIfNotExists() } returns true

    val clientState = AzureStorageClient(config = config, type = DocumentType.STATE, azureClient = azureClient)
    assertEquals("/state/$KEY", clientState.key(KEY))

    val workloadState = AzureStorageClient(config = config, type = DocumentType.WORKLOAD_OUTPUT, azureClient = azureClient)
    assertEquals("/workload/output/$KEY", workloadState.key(KEY))
  }

  @Test
  fun `read missing doc`() {
    val azureClient: BlobServiceClient = mockk()
    val blobContainerClient: BlobContainerClient = mockk()

    every { azureClient.getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns blobContainerClient
    every { blobContainerClient.exists() } returns false
    every { blobContainerClient.createIfNotExists() } returns true

    val client = AzureStorageClient(config = config, type = DocumentType.STATE, azureClient = azureClient)

    every { azureClient.getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns
      mockk {
        every { getBlobClient(client.key(KEY)) } returns
          mockk {
            every { exists() } returns false
          }
      }

    assertNull(client.read(KEY), "key $KEY should be null")
  }

  @Test
  fun `read existing doc`() {
    val azureClient: BlobServiceClient = mockk()
    val blobContainerClient: BlobContainerClient = mockk()

    every { azureClient.getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns blobContainerClient
    every { blobContainerClient.exists() } returns false
    every { blobContainerClient.createIfNotExists() } returns true

    val client = AzureStorageClient(config = config, type = DocumentType.STATE, azureClient = azureClient)

    every { azureClient.getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns
      mockk {
        every { getBlobClient(client.key(KEY)) } returns
          mockk {
            every { exists() } returns true
            every { downloadContent() } returns
              mockk<BinaryData> BinaryData@{
                every { this@BinaryData.toString() } returns DOC1
              }
          }
      }

    with(client.read(KEY)) {
      assertNotNull(this, "key $KEY should not be null")
      assertEquals(DOC1, this)
    }
  }

  @Test
  fun `write doc`() {
    val azureClient: BlobServiceClient = mockk()
    val blobClient: BlobClient = mockk()
    val blobContainerClient: BlobContainerClient = mockk()

    every { azureClient.getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns blobContainerClient
    every { blobContainerClient.exists() } returns false
    every { blobContainerClient.createIfNotExists() } returns true

    every { blobClient.exists() } returns true
    every { blobClient.upload(any<InputStream>()) } returns Unit

    val client = AzureStorageClient(config = config, type = DocumentType.STATE, azureClient = azureClient)
    every { blobContainerClient.getBlobClient(client.key(KEY)) } returns blobClient

    client.write(KEY, DOC1)
    verify { blobClient.upload(any<InputStream>()) }
  }

  @Test
  fun `delete doc`() {
    val azureClient: BlobServiceClient = mockk()
    val blobContainerClient: BlobContainerClient = mockk()

    every { azureClient.getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns blobContainerClient
    every { blobContainerClient.exists() } returns false
    every { blobContainerClient.createIfNotExists() } returns true

    val client = AzureStorageClient(config = config, type = DocumentType.STATE, azureClient = azureClient)

    // doc not deleted
    every { azureClient.getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns
      mockk {
        every { getBlobClient(client.key(KEY)) } returns
          mockk {
            every { deleteIfExists() } returns false
          }
      }
    assertFalse(client.delete(KEY))

    // doc deleted
    every { azureClient.getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns
      mockk {
        every { getBlobClient(client.key(KEY)) } returns
          mockk {
            every { deleteIfExists() } returns true
          }
      }
    assertTrue(client.delete(KEY))
  }

  @Test
  fun `list docs`() {
    val files = listOf("file1", "file2", "file3")
    val blobs =
      files.map {
        mockk<BlobItem> {
          every { name } returns it
        }
      }
    val blobContainerClient: BlobContainerClient =
      mockk {
        every { listBlobsByHierarchy(any<String>()) } returns
          mockk {
            every { iterator() } returns blobs.toMutableList().iterator()
          }
      }

    val azureClient: BlobServiceClient =
      mockk {
        every { getBlobContainerClient(config.bucketName(DocumentType.STATE)) } returns blobContainerClient
      }

    val client = AzureStorageClient(config = config, type = DocumentType.STATE, azureClient = azureClient)

    val result = client.list(id = KEY)
    assertEquals(files, result)
  }
}

internal class GcsStorageClientTest {
  private val config = GcsStorageConfig(buckets = buckets, applicationCredentials = "app-creds")

  @Test
  fun `blobId matches`() {
    val gcsClient: Storage =
      mockk {
        every { get(any<String>(), *anyVararg()) } returns null
        every { create(any<BucketInfo>()) } returns mockk<GcsBucket>()
      }

    val clientState = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)
    assertEquals(BlobId.of(buckets.state, "/state/$KEY"), clientState.blobId(KEY))

    val workloadState = GcsStorageClient(config = config, type = DocumentType.WORKLOAD_OUTPUT, gcsClient = gcsClient)
    assertEquals(BlobId.of(buckets.workloadOutput, "/workload/output/$KEY"), workloadState.blobId(KEY))
  }

  @Test
  fun `read missing doc`() {
    val gcsClient: Storage =
      mockk {
        every { get(config.bucketName(DocumentType.STATE), *anyVararg()) } returns null
        every { create(any<BucketInfo>()) } returns mockk<GcsBucket>()
      }
    val client = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)

    // verify no blob is returned
    every { gcsClient.get(any<BlobId>()) } returns null
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
    val gcsClient: Storage =
      mockk {
        every { get(config.bucketName(DocumentType.STATE), *anyVararg()) } returns null
        every { create(any<BucketInfo>()) } returns mockk<GcsBucket>()
      }
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
    val gcsClient: Storage =
      mockk {
        every { get(config.bucketName(DocumentType.STATE), *anyVararg()) } returns null
        every { create(any<BucketInfo>()) } returns mockk<GcsBucket>()
      }
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
    val gcsClient: Storage =
      mockk {
        every { get(config.bucketName(DocumentType.STATE), *anyVararg()) } returns null
        every { create(any<BucketInfo>()) } returns mockk<GcsBucket>()
      }
    val client = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)

    val blobId = client.blobId(KEY)

    // doc not deleted
    every { gcsClient.delete(blobId) } returns false
    assertFalse(client.delete(KEY))

    // doc deleted
    every { gcsClient.delete(blobId) } returns true
    assertTrue(client.delete(KEY))
  }

  @Test
  fun `list docs`() {
    // GCS returns files in reversed order
    val files = listOf("file3", "file2", "file1")
    val blobs =
      files.map {
        mockk<Blob> {
          every { name } returns it
        }
      }
    val page =
      mockk<Page<Blob>> {
        every { iterateAll() } returns blobs
      }
    val gcsClient =
      mockk<Storage> {
        every { list(any<String>(), any()) } returns page
      }

    val client = GcsStorageClient(config = config, type = DocumentType.STATE, gcsClient = gcsClient)

    val result = client.list(id = KEY)
    assertEquals(files, result)
  }
}

internal class LocalStorageClientTest {
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

  @Test
  fun `list docs`() {
    val root = createTempDirectory(prefix = "local-test")
    // Create subdirectory to ensure it is not included in list results
    createTempDirectory(directory = root, prefix = "subdir-test")
    val stateDir = Path.of(root.pathString, "state")
    createDirectory(stateDir)
    val file = createTempFile(directory = stateDir, suffix = ".log")
    file.toFile().writeText(text = "log line")

    val config = LocalStorageConfig(buckets = buckets, root = root.pathString)
    val client = LocalStorageClient(config = config, type = DocumentType.STATE)

    val result = client.list("/")
    assertEquals(listOf(file.fileName.toString()), result)
  }

  @Test
  fun `it can write and read state files correctly`() {
    val root = createTempDirectory(prefix = "local-test")
    val config = LocalStorageConfig(buckets = buckets, root = root.pathString)
    val client = LocalStorageClient(config = config, type = DocumentType.STATE)

    client.write("foo", "foodoc")
    client.write("bar/baz", "barbaz")
    client.write("job/0/log1", "log1")
    client.write("job/0/log2", "log2")
    client.write("job/0/sub/log3", "log3")

    assertEquals("foodoc", client.read("foo"))
    assertEquals("barbaz", client.read("bar/baz"))
    assertEquals("log1", client.read("job/0/log1"))

    // note that list() does NOT recurse into subdirectories
    assertEquals(listOf("job/0/log1", "job/0/log2"), client.list("job/0"))

    // if the ID looks like an absolute path, it still writes to the local storage dir
    val now = System.currentTimeMillis()
    client.write("/boo-$now", "boo")
    assertTrue(Path.of(root.pathString, "state/boo-$now").exists())
  }

  @Test
  internal fun testToEnvVarMap() {
    val root = "/root/path"
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        // Audit logging is null by default as it is SME feature only
        auditLogging = null,
      )
    val localStorageConfig =
      LocalStorageConfig(
        buckets = bucketConfig,
        root = root,
      )
    val envVarMap = localStorageConfig.toEnvVarMap()
    assertEquals(5, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(StorageType.LOCAL.name, envVarMap[EnvVar.STORAGE_TYPE.name])
  }

  @Test
  internal fun testToEnvVarMapWithAuditLogging() {
    val root = "/root/path"
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        auditLogging = "audit-logging",
      )
    val localStorageConfig =
      LocalStorageConfig(
        buckets = bucketConfig,
        root = root,
      )
    val envVarMap = localStorageConfig.toEnvVarMap()
    assertEquals(6, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(StorageType.LOCAL.name, envVarMap[EnvVar.STORAGE_TYPE.name])
  }
}

internal class MinioStorageClientTest {
  private val config = MinioStorageConfig(buckets = buckets, accessKey = "access", secretAccessKey = "secret", endpoint = "endpoint")

  @Test
  fun `key matches`() {
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }

    val clientState = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)
    assertEquals("/state/$KEY", clientState.key(KEY))

    val workloadState = MinioStorageClient(config = config, type = DocumentType.WORKLOAD_OUTPUT, s3Client = s3Client)
    assertEquals("/workload/output/$KEY", workloadState.key(KEY))
  }

  @Test
  fun `read missing doc`() {
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }
    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      GetObjectRequest
        .builder()
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
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }
    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      GetObjectRequest
        .builder()
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
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }
    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      PutObjectRequest
        .builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()

    // It appears that RequestBody.fromString(DOC1) does not equal RequestBody.fromString(DOC1), hence the any() usage here
    every { s3Client.putObject(request, any<RequestBody>()) } returns mockk()

    client.write(KEY, DOC1)
  }

  @Test
  fun `delete doc`() {
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }
    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val existsRequest =
      HeadObjectRequest
        .builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()
    val deleteRequest =
      DeleteObjectRequest
        .builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()
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

  @Test
  fun `list docs`() {
    val files = listOf("file1", "file2", "file3")
    val keys =
      files.map {
        mockk<S3Object> {
          every { key() } returns it
        }
      }
    val page =
      mockk<ListObjectsV2Response> {
        every { contents() } returns keys
      }
    val iterable =
      mockk<ListObjectsV2Iterable> {
        every { iterator() } returns mutableListOf(page).iterator()
      }
    val s3Client =
      mockk<S3Client> {
        every { listObjectsV2Paginator(any<ListObjectsV2Request>()) } returns iterable
      }

    val client = MinioStorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val result = client.list(id = KEY)
    assertEquals(files, result)
  }
}

internal class S3StorageClientTest {
  private val config = S3StorageConfig(buckets = buckets, accessKey = "access", secretAccessKey = "secret", region = "us-east-1")

  @Test
  fun `key matches`() {
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }

    val clientState = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)
    assertEquals("/state/$KEY", clientState.key(KEY))

    val workloadState = S3StorageClient(config = config, type = DocumentType.WORKLOAD_OUTPUT, s3Client = s3Client)
    assertEquals("/workload/output/$KEY", workloadState.key(KEY))
  }

  @Test
  fun `read missing doc`() {
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }
    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      GetObjectRequest
        .builder()
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
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }
    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      GetObjectRequest
        .builder()
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
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }
    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val request =
      PutObjectRequest
        .builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()

    // It appears that RequestBody.fromString(DOC1) does not equal RequestBody.fromString(DOC1), hence the any() usage here
    every { s3Client.putObject(request, any<RequestBody>()) } returns mockk()

    client.write(KEY, DOC1)
  }

  @Test
  fun `delete doc`() {
    val s3Client: S3Client =
      mockk {
        every { createBucket(any<CreateBucketRequest>()) } returns mockk<CreateBucketResponse>()
        every { headBucket(any<HeadBucketRequest>()) } throws NoSuchBucketException.builder().build()
      }
    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val existsRequest =
      HeadObjectRequest
        .builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()
    val deleteRequest =
      DeleteObjectRequest
        .builder()
        .bucket(buckets.state)
        .key(client.key(KEY))
        .build()
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

  @Test
  fun `list docs`() {
    val files = listOf("file1", "file2", "file3")
    val keys =
      files.map {
        mockk<S3Object> {
          every { key() } returns it
        }
      }
    val page =
      mockk<ListObjectsV2Response> {
        every { contents() } returns keys
      }
    val iterable =
      mockk<ListObjectsV2Iterable> {
        every { iterator() } returns mutableListOf(page).iterator()
      }
    val s3Client =
      mockk<S3Client> {
        every { listObjectsV2Paginator(any<ListObjectsV2Request>()) } returns iterable
      }

    val client = S3StorageClient(config = config, type = DocumentType.STATE, s3Client = s3Client)

    val result = client.list(id = KEY)
    assertEquals(files, result)
  }
}
