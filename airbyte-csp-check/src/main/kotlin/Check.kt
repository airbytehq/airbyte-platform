package io.airbyte.commons.env

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
internal val storageDocTypes = listOf(DocumentType.STATE, DocumentType.LOGS, DocumentType.WORKLOAD_OUTPUT, DocumentType.ACTIVITY_PAYLOADS)

/**
 * Holds the results of the various environmental checks executed.
 *
 * @property storage are the results of the storage checks performed.
 */
data class CheckResult(
  val storage: Storage,
)

/**
 * Represents the storage checks.
 *
 * @property type the storage type this check represents
 * @property results contains the results from the storage checks
 */
data class Storage(
  val type: StorageType,
  val results: Section,
)

/** Section represents a grouping of [Permission] objects into a named section. */
typealias Section = Map<String, Permission>

/**
 * Permission represents the status of a named check
 * @TODO: rename this to something better
 **/
typealias Permission = Map<String, Status>

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
class Check(
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
      .map { checkStoragePermissions(it) }
      .reduce { a, b -> a + b }
      .let {
        Storage(type = storageType, results = it)
      }
}

/**
 * Runs a series of storage specific checks, returning a list of check statuses.
 */
private fun checkStoragePermissions(client: StorageClient): Section {
  val permissions = mutableMapOf<String, Status>()
  toPermission("write") { client.write(STORAGE_DOC_ID, STORAGE_DOC_CONTENTS) }.let { permissions += it }
  toPermission("read") {
    client.read(STORAGE_DOC_ID).also {
      if (it != STORAGE_DOC_CONTENTS) {
        throw CheckException("read contents did not match written contents")
      }
    }
  }.let { permissions += it }
  toPermission("list") { client.list(STORAGE_DOC_ID) }.let { permissions += it }
  toPermission("delete") { client.delete(STORAGE_DOC_ID) }.let { permissions += it }

  return mapOf(client.documentType().toString() to permissions)
}

/**
 * Converts a [block] to a [GreenStatus] or [RedStatus].
 *
 * @param name is the name of this check.
 * @param block is the code to execute.
 *
 * If [block] throws an exception, the status of this check will be one of [RedStatus].
 * Otherwise, a [GreenStatus] will be returned.
 */
private inline fun <R> toPermission(
  name: String,
  block: () -> R,
): Permission =
  runCatching { block() }
    .fold(
      onSuccess = { GreenStatus() },
      onFailure = { RedStatus(throwable = it) },
    ).let {
      mapOf(name to it)
    }
