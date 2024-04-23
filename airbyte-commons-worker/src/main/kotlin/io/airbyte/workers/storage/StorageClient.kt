package io.airbyte.workers.storage

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.airbyte.commons.io.IOs
import io.airbyte.config.storage.GcsStorageConfig
import io.airbyte.config.storage.LocalStorageConfig
import io.airbyte.config.storage.MinioStorageConfig
import io.airbyte.config.storage.S3StorageConfig
import io.airbyte.config.storage.STORAGE_TYPE
import io.airbyte.config.storage.StorageConfig
import io.airbyte.config.storage.StorageType
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
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
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
  fun get(type: DocumentType): StorageClient =
    when (storageType) {
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
enum class DocumentType(val prefix: Path) {
  LOGS(prefix = Path.of("/job-logging")),
  STATE(prefix = Path.of("/state")),
  WORKLOAD_OUTPUT(prefix = Path.of("/workload/output")),
  ACTIVITY_PAYLOADS(prefix = Path.of("/activity-payloads")),
}

/**
 * Interface for writing, reading, and deleting documents.
 */
interface StorageClient {
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
  private val bucketName = config.bucketName(type)

  @Inject
  constructor(
    config: GcsStorageConfig,
    @Parameter type: DocumentType,
  ) : this(config = config, type = type, gcsClient = config.gcsClient())

  override fun write(
    id: String,
    document: String,
  ) {
    val blobInfo = BlobInfo.newBuilder(blobId(id)).build()
    gcsClient.create(blobInfo, document.toByteArray(StandardCharsets.UTF_8))
  }

  override fun read(id: String): String? {
    val blobId = blobId(id)

    return gcsClient.get(blobId)
      ?.takeIf { it.exists() }
      ?.let { gcsClient.readAllBytes(blobId).toString(StandardCharsets.UTF_8) }
  }

  override fun delete(id: String): Boolean = gcsClient.delete(BlobId.of(bucketName, key(id)))

  internal fun key(id: String): String = "${type.prefix}/$id"

  internal fun blobId(id: String): BlobId = BlobId.of(bucketName, key(id))
}

/**
 * Constructs a [LocalStorageClient] implementation of the [StorageClient].
 *
 * @param config The [LocalStorageConfig] for configuring this [StorageConfig]
 * @param type which [DocumentType] this client represents
 */
@Prototype
class LocalStorageClient(
  config: LocalStorageConfig,
  @Parameter type: DocumentType,
) : StorageClient {
  // internal for testing
  internal val root: Path = Path.of(config.root, type.prefix.toString())

  override fun write(
    id: String,
    document: String,
  ) {
    val path =
      path(id).also { it.createParentDirectories() }
    IOs.writeFile(path, document)
  }

  override fun read(id: String): String? =
    path(id)
      .takeIf { it.exists() }
      ?.let { IOs.readFile(it) }

  override fun delete(id: String): Boolean =
    path(id)
      .deleteIfExists()

  /** Converts [String] to a [Path] relative to the [root]. */
  private fun path(id: String): Path = root.resolve(id)
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
  private val bucketName = config.bucketName(type)

  override fun write(
    id: String,
    document: String,
  ) {
    val request =
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key(id))
        .build()

    s3Client.putObject(request, RequestBody.fromString(document))
  }

  override fun read(id: String): String? {
    return try {
      s3Client.getObjectAsBytes(
        GetObjectRequest.builder()
          .bucket(bucketName)
          .key(key(id))
          .build(),
      ).asString(StandardCharsets.UTF_8)
    } catch (e: NoSuchKeyException) {
      null
    }
  }

  override fun delete(id: String): Boolean {
    val exists =
      try {
        s3Client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(key(id)).build())
        true
      } catch (e: NoSuchKeyException) {
        false
      }

    s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(key(id)).build())
    return exists
  }

  internal fun key(id: String): String = "${type.prefix}/$id"
}

/**
 * Extension function for extracting a [Storage] client out of the [GcsStorageConfig].
 *
 * @receiver [GcsStorageConfig]
 * internal for mocking purposes
 */
internal fun GcsStorageConfig.gcsClient(): Storage {
  val credentialsByteStream = ByteArrayInputStream(Files.readAllBytes(Path.of(this.applicationCredentials)))
  val credentials = ServiceAccountCredentials.fromStream(credentialsByteStream)
  return StorageOptions.newBuilder().setCredentials(credentials).build().service
}

/**
 * Extension function for extracting a [Storage] client out of the [MinioStorageConfig].
 *
 * @receiver [MinioStorageConfig]
 */
internal fun MinioStorageConfig.s3Client(): S3Client =
  S3Client.builder()
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
  }
