package io.airbyte.commons.csp

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.STORAGE_TYPE
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.commons.storage.StorageType
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

/** The document-id used by the storage checks. */
@InternalForTesting
internal const val STORAGE_DOC_ID = ".airbyte-env-check"

/** The document contents used by the storage checks. */
@InternalForTesting
internal const val STORAGE_DOC_CONTENTS = "environment permissions check"

/**
 * The [StorageClient] implementation is currently based on the [DocumentType] being used.
 * To ensure that all of the proper storage checks are performed, each [DocumentType]'s [StorageClient] needs to be checked.
 */
@InternalForTesting
internal val storageDocTypes =
  listOf(DocumentType.STATE, DocumentType.LOGS, DocumentType.WORKLOAD_OUTPUT, DocumentType.ACTIVITY_PAYLOADS, DocumentType.AUDIT_LOGS)

/**
 * Holds the results of the various environmental checks executed.
 *
 * @property storage are the results of the storage checks performed.
 */
data class CheckResult(
  val storage: Storage,
)

/** Aliasing an [Action] to a string */
typealias Action = String

/**
 * Represents the storage checks.
 *
 * @property type the storage type this check represents
 * @property buckets contains the results from the storage checks
 */
data class Storage(
  val type: StorageType,
  val buckets: List<Bucket>,
)

/**
 * Represents a collection of bucket checks.
 *
 * @property name is the name of the bucket
 * @property documentType is the [DocumentType] this bucket represents
 * @property results are the result of the [Action] and its resulting [Status]
 */
data class Bucket(
  val name: String,
  val documentType: DocumentType,
  val results: Map<Action, Status>,
)

/**
 * An exception that can be thrown by one of the various checks executed in this package.
 */
internal class CheckException(
  msg: String,
) : RuntimeException(msg)

/**
 * Class that supports running the checks
 *
 * @property storageType injected value which indicates which [StorageType] these checks should run with.
 * @property storageFactory injected instance of the [StorageClientFactory] which is used to create the [StorageClient].
 */
@Singleton
class CspChecker(
  @Value("\${$STORAGE_TYPE}") private val storageType: StorageType,
  private val storageFactory: StorageClientFactory,
) {
  /**
   * The method which performs the environment checks.
   */
  fun check(): CheckResult = CheckResult(storage = checkStorage())

  /** Performs the storage checks. */
  private fun checkStorage(): Storage =
    storageDocTypes
      .map { storageFactory.create(it) }
      .map { checkBucket(it) }
      .toList()
      .let {
        Storage(type = storageType, buckets = it)
      }
}

/**
 * Runs a series of storage specific checks, returning a [Bucket] response.
 */
private fun checkBucket(client: StorageClient): Bucket {
  val results =
    mapOf(
      toStatus("write") { client.write(STORAGE_DOC_ID, STORAGE_DOC_CONTENTS) },
      toStatus("read") {
        client.read(STORAGE_DOC_ID).also {
          if (it != STORAGE_DOC_CONTENTS) {
            throw CheckException("read contents did not match written contents")
          }
        }
      },
      toStatus("list") { client.list(STORAGE_DOC_ID) },
      toStatus("delete") { client.delete(STORAGE_DOC_ID) },
    )

  return Bucket(
    name = client.bucketName,
    documentType = client.documentType,
    results = results,
  )
}

/**
 * Converts a [block] to a [Status].
 *
 * @param name
 * @param block is the code to execute
 *
 * If [block] throws an exception, the status of this check will be one of [FailStatus].
 * Otherwise, a [PassStatus] will be returned.
 */
private inline fun <R> toStatus(
  name: String,
  block: () -> R,
): Pair<String, Status> =
  runCatching { block() }
    .fold(
      onSuccess = { PassStatus() },
      onFailure = { FailStatus(throwable = it) },
    ).let {
      Pair(name, it)
    }
