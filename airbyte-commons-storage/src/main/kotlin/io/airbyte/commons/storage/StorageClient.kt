/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.io.IOs
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Parameter
import io.micronaut.context.annotation.Prototype
import io.micronaut.context.annotation.Value
import io.micronaut.kotlin.context.createBean
import jakarta.inject.Inject
import jakarta.inject.Singleton
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createParentDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.pathString
import kotlin.io.path.relativeTo

private fun prependIfMissing(
  prefix: String,
  id: String,
) = if (id.startsWith(prefix)) id else "${prefix.trimEnd('/')}/${id.trimStart('/')}"

enum class StorageType {
  AZURE,
  GCS,
  LOCAL,
  MINIO,
  S3,
}

/**
 * Factory for creating a [StorageClient] based on the value of [STORAGE_TYPE] and a [DocumentType].
 */
@Singleton
class StorageClientFactory(
  private val appCtx: ApplicationContext,
  @Value("\${$STORAGE_TYPE}") private val storageType: StorageType,
) {
  /**
   * Returns a [StorageClient] for the specified [type]
   */
  fun create(type: DocumentType): StorageClient =
    when (storageType) {
      StorageType.AZURE -> appCtx.createBean<AzureStorageClient>(type)
      StorageType.GCS -> appCtx.createBean<GcsStorageClient>(type)
      StorageType.LOCAL -> appCtx.createBean<LocalStorageClient>(type)
      StorageType.MINIO -> appCtx.createBean<MinioStorageClient>(type)
      StorageType.S3 -> appCtx.createBean<S3StorageClient>(type)
    }
}

/**
 * What type of document is the [StorageClient] handling.
 *
 * @param prefix The prefix to which these documents will be stored
 */
enum class DocumentType(
  val prefix: Path,
) {
  // Note that job logs should not have a leading slash to ensure
  // that GCS/Azure can find these files in blob storage.  Both of those
  // cloud providers treat the leading slash as a directory.  Currently, logs
  // are retrieved by the LogClient, which uses a path set on the attempt that
  // does NOT contain a leading slash.  Therefore, these paths need to match that logic.
  LOGS(prefix = Path.of("job-logging")),
  STATE(prefix = Path.of("/state")),
  WORKLOAD_OUTPUT(prefix = Path.of("/workload/output")),
  ACTIVITY_PAYLOADS(prefix = Path.of("/activity-payloads")),
  AUDIT_LOGS(prefix = Path.of("audit-logging")),
  REPLICATION_DUMP(prefix = Path.of("replication-dump")),
  PROFILER_OUTPUT(prefix = Path.of("/profiler/output")),
}

/**
 * Interface for writing, reading, and deleting documents.
 */
interface StorageClient {
  /** @property documentType the [DocumentType] of this [StorageClient] */
  val documentType: DocumentType

  /** @property storageType the [StorageType] of this [StorageClient] */
  val storageType: StorageType

  /** @property bucketName the name of the bucket used by this [StorageClient] */
  val bucketName: String

  /**
   * Lists the documents stored at the given id.
   *
   * @param id of the document path
   * @return the list of documents at the provided id.
   */
  fun list(id: String): List<String>

  /**
   * Writes a document with a given id. If a document already exists at this id it will be
   * overwritten.
   *
   * @param id of the document to write
   * @param document to write
   */
  fun write(
    id: String,
    document: String,
  )

  /**
   * Reads document with a given id.
   *
   * @param id of the document to read.
   * @return the document
   */
  fun read(id: String): String?

  /**
   * Deletes the document with provided id.
   *
   * @param id of document to delete
   * @return true if deletes something, otherwise false.
   */
  fun delete(id: String): Boolean

  /**
   * Generates a file ID.
   *
   * @param id a relative file path
   * @return the file ID including any configured storage prefix
   */
  fun key(id: String): String = prependIfMissing(prefix = documentType.prefix.toString(), id = id)
}

/**
 * Constructs a [AzureStorageClient] implementation of the [StorageClient].
 *
 * @param config The [AzureStorageConfig] for configuring this [StorageConfig]
 * @param type which [DocumentType] this client represents
 * @param azureClient the [Storage] client, should only be specified for testing purposes
 */
@Prototype
class AzureStorageClient(
  config: AzureStorageConfig,
  private val type: DocumentType,
  private val azureClient: BlobServiceClient,
) : StorageClient {
  override val storageType = StorageType.AZURE
  override val documentType = type
  override val bucketName = config.bucketName(type)

  @Inject
  constructor(
    config: AzureStorageConfig,
    @Parameter type: DocumentType,
  ) : this(config = config, type = type, azureClient = config.azureClient())

  init {
    runCatching { createBucketIfNotExists() }
  }

  override fun list(id: String): List<String> {
    val blobKey = key(id)
    // azure needs the trailing `/` in order for the list call to return the correct blobs
    val directory = if (blobKey.endsWith("/")) blobKey else "$blobKey/"
    return azureClient
      .getBlobContainerClient(bucketName)
      .listBlobsByHierarchy(directory)
      .map { it.name }
      .sortedBy { it }
  }

  override fun write(
    id: String,
    document: String,
  ) {
    azureClient
      .getBlobContainerClient(bucketName)
      .getBlobClient(key(id))
      .upload(document.byteInputStream(StandardCharsets.UTF_8))
  }

  override fun read(id: String): String? =
    azureClient
      .getBlobContainerClient(bucketName)
      .getBlobClient(key(id))
      // ensure the blob exists before downloading it
      .takeIf { it.exists() }
      ?.downloadContent()
      ?.toString()

  override fun delete(id: String): Boolean =
    azureClient
      .getBlobContainerClient(bucketName)
      .getBlobClient(key(id))
      .deleteIfExists()

  private fun createBucketIfNotExists() {
    val blobContainerClient = azureClient.getBlobContainerClient(bucketName)
    if (!blobContainerClient.exists()) {
      blobContainerClient.createIfNotExists()
    }
  }
}

/**
 * Constructs a [GcsStorageClient] implementation of the [StorageClient].
 *
 * @param config The [GcsStorageConfig] for configuring this [StorageConfig]
 * @param type which [DocumentType] this client represents
 * @param gcsClient the [Storage] client, should only be specified for testing purposes
 */
@Prototype
class GcsStorageClient(
  config: GcsStorageConfig,
  private val type: DocumentType,
  private val gcsClient: Storage,
) : StorageClient {
  override val storageType = StorageType.GCS
  override val documentType = type
  override val bucketName = config.bucketName(type)

  @Inject
  constructor(
    config: GcsStorageConfig,
    @Parameter type: DocumentType,
  ) : this(config = config, type = type, gcsClient = config.gcsClient())

  init {
    runCatching { createBucketIfNotExists() }
  }

  override fun list(id: String): List<String> =
    gcsClient
      .list(
        bucketName,
        Storage.BlobListOption.prefix(key(id)),
      ).iterateAll()
      .map { it.name }

  override fun write(
    id: String,
    document: String,
  ) {
    val blobInfo = BlobInfo.newBuilder(blobId(id)).build()
    gcsClient.create(blobInfo, document.toByteArray(StandardCharsets.UTF_8))
  }

  override fun read(id: String): String? {
    val blobId = blobId(key(id))

    return gcsClient
      .get(blobId)
      ?.takeIf { it.exists() }
      ?.let { gcsClient.readAllBytes(it.blobId).toString(StandardCharsets.UTF_8) }
  }

  override fun delete(id: String): Boolean = gcsClient.delete(BlobId.of(bucketName, key(id)))

  @VisibleForTesting
  internal fun blobId(id: String): BlobId = BlobId.of(bucketName, key(id))

  private fun createBucketIfNotExists() {
    if (gcsClient.get(bucketName) == null) {
      gcsClient.create(BucketInfo.of(bucketName))
    }
  }
}

/**
 * Constructs a [LocalStorageClient] implementation of the [StorageClient].
 *
 * @param config The [LocalStorageConfig] for configuring this [StorageConfig]
 * @param type which [DocumentType] this client represents
 */
@Prototype
class LocalStorageClient(
  private val config: LocalStorageConfig,
  @Parameter private val type: DocumentType,
) : StorageClient {
  override val storageType = StorageType.LOCAL
  override val documentType = type
  override val bucketName = config.bucketName(type)

  override fun list(id: String): List<String> {
    val res =
      toPath(id)
        .takeIf { it.exists() }
        ?.listDirectoryEntries()
        ?.filter { !it.isDirectory() }
        ?.map { toId(it) }
        ?: emptyList()
    return res.sorted()
  }

  override fun write(
    id: String,
    document: String,
  ) {
    val path =
      toPath(id).also { it.createParentDirectories() }
    IOs.writeFile(path, document)
  }

  override fun read(id: String): String? =
    toPath(id)
      .takeIf { it.exists() }
      ?.let { IOs.readFile(it) }

  override fun delete(id: String): Boolean =
    toPath(id)
      .deleteIfExists()

  /** Converts an ID [String] to an absolute [Path]. */
  internal fun toPath(id: String): Path = Path.of(config.root, type.prefix.toString(), id)

  /** Converts an absolute [Path] to an ID [String]. */
  private fun toId(abspath: Path): String = abspath.relativeTo(Path.of(config.root, type.prefix.toString())).pathString
}

/**
 * Constructs a [MinioStorageClient] implementation of the [StorageClient].
 *
 * @param config The [MinioStorageClient] for configuring this [StorageConfig]
 * @param type which [DocumentType] this client represents
 * @param s3Client the [S3Client] client, should only be specified for testing purposes
 */
@Prototype
class MinioStorageClient(
  config: MinioStorageConfig,
  type: DocumentType,
  s3Client: S3Client = config.s3Client(),
) : AbstractS3StorageClient(config = config, type = type, s3Client = s3Client) {
  override val storageType = StorageType.MINIO

  @Inject
  constructor(
    config: MinioStorageConfig,
    @Parameter type: DocumentType,
  ) : this(config = config, type = type, s3Client = config.s3Client())
}

/**
 * Constructs a [S3StorageClient] implementation of the [StorageClient].
 *
 * @param config The [S3StorageClient] for configuring this [StorageConfig]
 * @param type which [DocumentType] this client represents
 * @param s3Client the [S3Client] client, should only be specified for testing purposes
 */
@Prototype
class S3StorageClient(
  config: S3StorageConfig,
  type: DocumentType,
  s3Client: S3Client = config.s3Client(),
) : AbstractS3StorageClient(config = config, type = type, s3Client = s3Client) {
  @Inject
  constructor(
    config: S3StorageConfig,
    @Parameter type: DocumentType,
  ) : this(config = config, type = type, s3Client = config.s3Client())

  override val storageType = StorageType.S3
}

/**
 * Abstract class for creating an S3-ish [StorageClient].
 *
 * As both [MinioStorageClient] and [S3StorageClient] adhere to the S3 API, this consolidates these to a single abstract class.
 *
 * @param config The [StorageConfig] for configuring this [StorageClient]
 * @param type which [DocumentType] this client represents
 * @param s3Client the [S3Client] client, should only be specified for testing purposes
 */
abstract class AbstractS3StorageClient internal constructor(
  config: StorageConfig,
  private val type: DocumentType,
  private val s3Client: S3Client,
) : StorageClient {
  override val documentType: DocumentType = type
  override val bucketName = config.bucketName(type)

  init {
    runCatching { createBucketIfNotExists() }
  }

  override fun list(id: String): List<String> {
    val listObjReq =
      ListObjectsV2Request
        .builder()
        .bucket(bucketName)
        .prefix(key(id))
        .build()

    return s3Client
      .listObjectsV2Paginator(listObjReq)
      // Objects are returned in lexicographical order.
      .flatMap { it.contents() }
      .map { it.key() }
      .toList()
  }

  override fun write(
    id: String,
    document: String,
  ) {
    val request =
      PutObjectRequest
        .builder()
        .bucket(bucketName)
        .key(key(id))
        .build()

    s3Client.putObject(request, RequestBody.fromString(document))
  }

  override fun read(id: String): String? =
    try {
      s3Client
        .getObjectAsBytes(
          GetObjectRequest
            .builder()
            .bucket(bucketName)
            .key(key(id))
            .build(),
        ).asString(StandardCharsets.UTF_8)
    } catch (e: NoSuchKeyException) {
      null
    }

  override fun delete(id: String): Boolean {
    val exists =
      try {
        s3Client.headObject(
          HeadObjectRequest
            .builder()
            .bucket(bucketName)
            .key(key(id))
            .build(),
        )
        true
      } catch (e: NoSuchKeyException) {
        false
      }

    s3Client.deleteObject(
      DeleteObjectRequest
        .builder()
        .bucket(bucketName)
        .key(key(id))
        .build(),
    )
    return exists
  }

  private fun createBucketIfNotExists() {
    if (!doesBucketExist(bucketName = bucketName)) {
      val createBucketRequest = CreateBucketRequest.builder().bucket(bucketName).build()
      s3Client.createBucket(createBucketRequest)
    }
  }

  private fun doesBucketExist(bucketName: String): Boolean {
    val headBucketRequest = HeadBucketRequest.builder().bucket(bucketName).build()
    return try {
      s3Client.headBucket(headBucketRequest)
      true
    } catch (e: Exception) {
      false
    }
  }
}

/**
 * Extension function for extracting a [Storage] client out of the [GcsStorageConfig].
 *
 * @receiver [GcsStorageConfig]
 * internal for mocking purposes
 */
internal fun AzureStorageConfig.azureClient(): BlobServiceClient =
  BlobServiceClientBuilder()
    .connectionString(this@azureClient.connectionString)
    .buildClient()

/**
 * Extension function for extracting a [Storage] client out of the [GcsStorageConfig].
 *
 * @receiver [GcsStorageConfig]
 * internal for mocking purposes
 */
internal fun GcsStorageConfig.gcsClient(): Storage {
  val credentialsByteStream = ByteArrayInputStream(Files.readAllBytes(Path.of(this.applicationCredentials)))
  val credentials = ServiceAccountCredentials.fromStream(credentialsByteStream)
  return StorageOptions
    .newBuilder()
    .setCredentials(credentials)
    .build()
    .service
}

/**
 * Extension function for extracting a [Storage] client out of the [MinioStorageConfig].
 *
 * @receiver [MinioStorageConfig]
 */
internal fun MinioStorageConfig.s3Client(): S3Client =
  S3Client
    .builder()
    .serviceConfiguration { it.pathStyleAccessEnabled(true) }
    .credentialsProvider { AwsBasicCredentials.create(this@s3Client.accessKey, this@s3Client.secretAccessKey) }
    .endpointOverride(URI(this@s3Client.endpoint))
    // The region isn't actually used but is required. Set to us-east-1 based on https://github.com/minio/minio/discussions/15063.
    .region(Region.US_EAST_1)
    .build()

/**
 * Extension function for extracting a [Storage] client out of the [S3StorageConfig].
 *
 * @receiver [S3StorageConfig]
 */
internal fun S3StorageConfig.s3Client(): S3Client {
  val builder = S3Client.builder().region(Region.of(this.region))

  // If credentials are part of this config, specify them. Otherwise, let the SDK default credential provider take over.
  if (!this.accessKey.isNullOrBlank()) {
    builder.credentialsProvider {
      AwsBasicCredentials.create(this.accessKey, this.secretAccessKey)
    }
  }

  return builder.build()
}

/**
 * Helper method to extract a bucket name for the given [DocumentType].
 */
fun StorageConfig.bucketName(type: DocumentType): String =
  when (type) {
    DocumentType.STATE -> this.buckets.state
    DocumentType.WORKLOAD_OUTPUT -> this.buckets.workloadOutput
    DocumentType.LOGS -> this.buckets.log
    DocumentType.ACTIVITY_PAYLOADS -> this.buckets.activityPayload
    DocumentType.AUDIT_LOGS -> this.buckets.auditLogging?.takeIf { it.isNotBlank() } ?: ""
    DocumentType.REPLICATION_DUMP -> this.buckets.replicationDump?.takeIf { it.isNotBlank() } ?: REPLICATION_DUMP
    DocumentType.PROFILER_OUTPUT -> this.buckets.profilerOutput?.takeIf { it.isNotBlank() } ?: ""
  }
