/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.csp

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.airbyte.micronaut.runtime.StorageType
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
 * Cloud Service Provider (CSP) environment validation checker for Airbyte deployments.
 *
 * This service validates that the Airbyte deployment has proper permissions and access
 * to essential cloud storage services. It performs comprehensive storage checks across
 * all document types to ensure the deployment can read, write, list, and delete files
 * as required for normal operation.
 *
 * The checker validates storage access for:
 * - Connection state storage
 * - Log storage
 * - Workload output storage
 * - Activity payload storage
 * - Audit log storage
 *
 * @property storageConfiguration injected [AirbyteStorageConfig]
 * @property storageFactory injected instance of the [StorageClientFactory] which is used to create the [StorageClient].
 */
@Singleton
class CspChecker(
  private val storageConfiguration: AirbyteStorageConfig,
  private val storageFactory: StorageClientFactory,
) {
  /**
   * Performs comprehensive environment validation checks.
   *
   * This method executes storage permission tests across all configured storage buckets
   * and document types. Each test validates the four core storage operations (read, write,
   * list, delete) required for Airbyte to function properly.
   *
   * @return CheckResult containing detailed results of all performed validation checks
   */
  fun check(): CheckResult = CheckResult(storage = checkStorage())

  /**
   * Executes storage validation checks across all document types.
   *
   * Creates storage clients for each document type and runs comprehensive
   * permission tests on each associated storage bucket.
   *
   * @return Storage object containing results for all tested buckets
   */
  private fun checkStorage(): Storage =
    storageDocTypes
      .map { storageFactory.create(it) }
      .map { checkBucket(it) }
      .toList()
      .let {
        Storage(type = storageConfiguration.type, buckets = it)
      }
}

/**
 * Validates storage permissions for a specific bucket by testing core operations.
 *
 * This function performs a comprehensive test of storage permissions by:
 * 1. Writing a test document to verify write permissions
 * 2. Reading the document back to verify read permissions and data integrity
 * 3. Listing the document to verify list permissions
 * 4. Deleting the document to verify delete permissions and cleanup
 *
 * Each operation is wrapped in error handling to capture specific failure modes.
 *
 * @param client The storage client configured for a specific document type and bucket
 * @return Bucket object containing test results for all storage operations
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
      onSuccess = { _ -> PassStatus() },
      onFailure = { FailStatus(throwable = it) },
    ).let {
      Pair(name, it)
    }
