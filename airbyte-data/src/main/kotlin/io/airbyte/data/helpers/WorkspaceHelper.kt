/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.helpers

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import io.airbyte.config.Job
import io.airbyte.config.Job.Companion.REPLICATION_TYPES
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID
import java.util.concurrent.CompletionException
import java.util.function.Supplier

/**
 * Helpers for interacting with Workspaces.
 */
@Singleton
class WorkspaceHelper(
  jobService: JobService,
  connectionService: ConnectionService,
  sourceService: SourceService,
  destinationService: DestinationService,
  operationService: OperationService,
  workspaceService: WorkspaceService,
) {
  private val sourceToWorkspaceCache: LoadingCache<UUID, UUID> =
    getExpiringCache { sourceId -> sourceService.getSourceConnection(sourceId).workspaceId }
  private val destinationToWorkspaceCache: LoadingCache<UUID, UUID> =
    getExpiringCache { destinationId -> destinationService.getDestinationConnection(destinationId).workspaceId }
  private val connectionToWorkspaceCache: LoadingCache<UUID, UUID> =
    getExpiringCache { connectionId ->
      val connection = connectionService.getStandardSync(connectionId)
      getWorkspaceForConnectionIgnoreExceptions(connection.sourceId, connection.destinationId)
    }
  private val operationToWorkspaceCache: LoadingCache<UUID, UUID> =
    getExpiringCache { operationId -> operationService.getStandardSyncOperation(operationId).workspaceId }
  private val jobToWorkspaceCache: LoadingCache<Long, UUID> =
    getExpiringCache { jobId ->
      val job =
        jobService.findById(jobId)
          ?: throw ConfigNotFoundException(Job::class.java.toString(), jobId.toString())
      if (REPLICATION_TYPES.contains(job.configType)) {
        getWorkspaceForConnectionIdIgnoreExceptions(UUID.fromString(job.scope))
      } else {
        throw IllegalArgumentException("Only sync/reset jobs are associated with workspaces! A ${job.configType} job was requested!")
      }
    }

  private val workspaceToOrganizationCache: LoadingCache<UUID, UUID> =
    getExpiringCache { workspaceId -> workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false).organizationId }

  /**
   * There are generally two kinds of helper methods present here. The first kind propagate exceptions
   * for the method backing the cache. The second ignores them. The former is meant to be used with
   * proper api calls, while the latter is meant to be use with asserts and precondtions checks.
   *
   *
   * In API calls, distinguishing between various exceptions helps return the correct status code.
   */
  fun getWorkspaceForSourceId(sourceId: UUID): UUID = handleCacheExceptions { sourceToWorkspaceCache.get(sourceId) }

  fun getWorkspaceForSourceIdIgnoreExceptions(sourceId: UUID): UUID = swallowExecutionException { getWorkspaceForSourceId(sourceId) }

  // DESTINATION ID
  fun getWorkspaceForDestinationId(destinationId: UUID): UUID = handleCacheExceptions { destinationToWorkspaceCache.get(destinationId) }

  fun getWorkspaceForDestinationIdIgnoreExceptions(destinationId: UUID): UUID =
    swallowExecutionException {
      destinationToWorkspaceCache.get(destinationId)
    }

  // JOB ID
  fun getWorkspaceForJobId(jobId: Long): UUID = handleCacheExceptions { jobToWorkspaceCache.get(jobId) }

  fun getWorkspaceForJobIdIgnoreExceptions(jobId: Long): UUID = swallowExecutionException { jobToWorkspaceCache.get(jobId) }

  // ORGANIZATION ID
  fun getOrganizationForWorkspace(workspaceId: UUID): UUID = swallowExecutionException { workspaceToOrganizationCache.get(workspaceId) }

  // CONNECTION ID

  /**
   * Get workspace id from source and destination. Verify that the source and destination are from the
   * same workspace. Fails if either source or destination id are invalid.
   *
   * @param sourceId source id
   * @param destinationId destination id
   * @return workspace id
   */
  fun getWorkspaceForConnection(
    sourceId: UUID,
    destinationId: UUID,
  ): UUID {
    val sourceWorkspace = getWorkspaceForSourceId(sourceId)
    val destinationWorkspace = getWorkspaceForDestinationId(destinationId)

    require(sourceWorkspace == destinationWorkspace) { "Source and destination must be from the same workspace!" }
    return sourceWorkspace
  }

  /**
   * Get workspace id from source and destination. Verify that the source and destination are from the
   * same workspace. Always compares source and destination workspaces even if there are errors while
   * fetching them.
   *
   *
   * I don't know why we want this.
   *
   * @param sourceId source id
   * @param destinationId destination id
   * @return workspace id
   */
  fun getWorkspaceForConnectionIgnoreExceptions(
    sourceId: UUID,
    destinationId: UUID,
  ): UUID {
    val sourceWorkspace = getWorkspaceForSourceIdIgnoreExceptions(sourceId)
    val destinationWorkspace = getWorkspaceForDestinationIdIgnoreExceptions(destinationId)

    require(sourceWorkspace == destinationWorkspace) { "Source and destination must be from the same workspace!" }
    return sourceWorkspace
  }

  fun getWorkspaceForConnectionId(connectionId: UUID): UUID = handleCacheExceptions { connectionToWorkspaceCache.get(connectionId) }

  fun getWorkspaceForConnectionIdIgnoreExceptions(connectionId: UUID): UUID =
    swallowExecutionException { connectionToWorkspaceCache.get(connectionId) }

  // OPERATION ID
  fun getWorkspaceForOperationId(operationId: UUID): UUID = handleCacheExceptions { operationToWorkspaceCache.get(operationId) }

  fun getWorkspaceForOperationIdIgnoreExceptions(operationId: UUID): UUID = swallowExecutionException { operationToWorkspaceCache.get(operationId) }

  companion object {
    private val log = KotlinLogging.logger {}

    private fun handleCacheExceptions(supplier: () -> UUID): UUID {
      try {
        return supplier()
      } catch (e: CompletionException) {
        log.error(e.cause) { "Error retrieving cache:" }
        if (e.cause is ConfigNotFoundException) {
          throw (e.cause as ConfigNotFoundException?)!!
        }
        if (e.cause is ConfigNotFoundException) {
          throw (e.cause as ConfigNotFoundException?)!!
        }
        if (e.cause is JsonValidationException) {
          throw (e.cause as JsonValidationException?)!!
        }
        throw RuntimeException(e.cause.toString(), e)
      }
    }

    private fun swallowExecutionException(supplier: Supplier<UUID>): UUID {
      try {
        return supplier.get()
      } catch (e: Throwable) {
        throw RuntimeException(e)
      }
    }

    private fun <K : Any, V : Any> getExpiringCache(cacheLoader: CacheLoader<K, V>): LoadingCache<K, V> =
      Caffeine
        .newBuilder()
        .maximumSize(20000)
        .build(cacheLoader)
  }
}
