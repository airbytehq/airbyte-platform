/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import com.google.common.base.Preconditions
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import io.airbyte.config.Job
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.function.Supplier

// todo (cgardens) - this class is in an unintuitive module. it is weird that you need to import
// scheduler:persistence in order to get workspace ids for configs (e.g. source). Our options are to
// split this helper by database or put it in a new module.

/**
 * Helpers for interacting with Workspaces.
 */
class WorkspaceHelper(
  jobPersistence: JobPersistence,
  connectionService: ConnectionService,
  sourceService: SourceService,
  destinationService: DestinationService,
  operationService: OperationService,
  workspaceService: WorkspaceService,
) {
  private val sourceToWorkspaceCache: LoadingCache<UUID, UUID>
  private val destinationToWorkspaceCache: LoadingCache<UUID, UUID>
  private val connectionToWorkspaceCache: LoadingCache<UUID, UUID>
  private val operationToWorkspaceCache: LoadingCache<UUID, UUID>
  private val jobToWorkspaceCache: LoadingCache<Long, UUID>

  private val workspaceToOrganizationCache: LoadingCache<UUID, UUID>

  init {
    this.sourceToWorkspaceCache =
      getExpiringCache(
        object : CacheLoader<UUID, UUID>() {
          @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
          override fun load(sourceId: UUID): UUID {
            val source = sourceService.getSourceConnection(sourceId)
            return source.workspaceId
          }
        },
      )

    this.destinationToWorkspaceCache =
      getExpiringCache(
        object : CacheLoader<UUID, UUID>() {
          @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
          override fun load(destinationId: UUID): UUID {
            val destination = destinationService.getDestinationConnection(destinationId)
            return destination.workspaceId
          }
        },
      )

    this.connectionToWorkspaceCache =
      getExpiringCache(
        object : CacheLoader<UUID, UUID>() {
          @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
          override fun load(connectionId: UUID): UUID {
            val connection = connectionService.getStandardSync(connectionId)
            val sourceId = connection.sourceId
            val destinationId = connection.destinationId
            return getWorkspaceForConnectionIgnoreExceptions(sourceId, destinationId)
          }
        },
      )

    this.operationToWorkspaceCache =
      getExpiringCache(
        object : CacheLoader<UUID, UUID>() {
          @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
          override fun load(operationId: UUID): UUID {
            val operation = operationService.getStandardSyncOperation(operationId)
            return operation.workspaceId
          }
        },
      )

    this.jobToWorkspaceCache =
      getExpiringCache(
        object : CacheLoader<Long, UUID>() {
          @Throws(ConfigNotFoundException::class, IOException::class)
          override fun load(jobId: Long): UUID {
            val job =
              jobPersistence.getJob(jobId)
                ?: throw ConfigNotFoundException(Job::class.java.toString(), jobId.toString())
            if (Job.REPLICATION_TYPES.contains(job.configType)) {
              return getWorkspaceForConnectionIdIgnoreExceptions(UUID.fromString(job.scope))
            } else {
              throw IllegalArgumentException("Only sync/reset jobs are associated with workspaces! A " + job.configType + " job was requested!")
            }
          }
        },
      )

    this.workspaceToOrganizationCache =
      getExpiringCache(
        object : CacheLoader<UUID, UUID>() {
          @Throws(Exception::class)
          override fun load(workspaceId: UUID): UUID = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false).organizationId
        },
      )
  }

  // SOURCE ID

  /**
   * There are generally two kinds of helper methods present here. The first kind propagate exceptions
   * for the method backing the cache. The second ignores them. The former is meant to be used with
   * proper api calls, while the latter is meant to be use with asserts and precondtions checks.
   *
   *
   * In API calls, distinguishing between various exceptions helps return the correct status code.
   */
  @Throws(ConfigNotFoundException::class, JsonValidationException::class)
  fun getWorkspaceForSourceId(sourceId: UUID): UUID = handleCacheExceptions { sourceToWorkspaceCache[sourceId] }

  fun getWorkspaceForSourceIdIgnoreExceptions(sourceId: UUID): UUID = swallowExecutionException { getWorkspaceForSourceId(sourceId) }

  // DESTINATION ID
  @Throws(JsonValidationException::class, ConfigNotFoundException::class)
  fun getWorkspaceForDestinationId(destinationId: UUID): UUID = handleCacheExceptions { destinationToWorkspaceCache[destinationId] }

  fun getWorkspaceForDestinationIdIgnoreExceptions(destinationId: UUID): UUID =
    swallowExecutionException { destinationToWorkspaceCache[destinationId] }

  // JOB ID
  @Throws(JsonValidationException::class, ConfigNotFoundException::class)
  fun getWorkspaceForJobId(jobId: Long): UUID = handleCacheExceptions { jobToWorkspaceCache[jobId] }

  fun getWorkspaceForJobIdIgnoreExceptions(jobId: Long): UUID = swallowExecutionException { jobToWorkspaceCache[jobId] }

  // ORGANIZATION ID
  fun getOrganizationForWorkspace(workspaceId: UUID): UUID = swallowExecutionException { workspaceToOrganizationCache[workspaceId] }

  // CONNECTION ID

  /**
   * Get workspace id from source and destination. Verify that the source and destination are from the
   * same workspace. Fails if either source or destination id are invalid.
   *
   * @param sourceId source id
   * @param destinationId destination id
   * @return workspace id
   */
  @Throws(JsonValidationException::class, ConfigNotFoundException::class)
  fun getWorkspaceForConnection(
    sourceId: UUID,
    destinationId: UUID,
  ): UUID {
    val sourceWorkspace = getWorkspaceForSourceId(sourceId)
    val destinationWorkspace = getWorkspaceForDestinationId(destinationId)

    Preconditions.checkArgument(sourceWorkspace == destinationWorkspace, "Source and destination must be from the same workspace!")
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

    Preconditions.checkArgument(sourceWorkspace == destinationWorkspace, "Source and destination must be from the same workspace!")
    return sourceWorkspace
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class)
  fun getWorkspaceForConnectionId(connectionId: UUID): UUID = handleCacheExceptions { connectionToWorkspaceCache[connectionId] }

  fun getWorkspaceForConnectionIdIgnoreExceptions(connectionId: UUID): UUID = swallowExecutionException { connectionToWorkspaceCache[connectionId] }

  // OPERATION ID
  @Throws(JsonValidationException::class, ConfigNotFoundException::class)
  fun getWorkspaceForOperationId(operationId: UUID): UUID = handleCacheExceptions { operationToWorkspaceCache[operationId] }

  fun getWorkspaceForOperationIdIgnoreExceptions(operationId: UUID): UUID = swallowExecutionException { operationToWorkspaceCache[operationId] }

  companion object {
    private val log = KotlinLogging.logger {}

    @Throws(ConfigNotFoundException::class, JsonValidationException::class)
    private fun handleCacheExceptions(supplier: () -> UUID): UUID {
      try {
        return supplier()
      } catch (e: ExecutionException) {
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

    private fun <K, V> getExpiringCache(cacheLoader: CacheLoader<K, V>): LoadingCache<K, V> =
      CacheBuilder
        .newBuilder()
        .maximumSize(20000)
        .build(cacheLoader)
  }
}
