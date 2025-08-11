/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionContextRead
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionSchedule
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.GetWebhookConfigRequest
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.commons.converters.toInternal
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.config.JobWebhookConfig
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.FieldSelectionWorkspaces.AddSchedulingJitter
import io.airbyte.featureflag.LoadShedSchedulerBackoffMinutes
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.UseNewCronScheduleCalculation
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workers.helpers.CronSchedulingHelper
import io.airbyte.workers.helpers.ScheduleJitterHelper
import io.airbyte.workers.input.InputFeatureFlagContextMapper
import io.airbyte.workers.temporal.activities.GetConnectionContextInput
import io.airbyte.workers.temporal.activities.GetConnectionContextOutput
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffInput
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffOutput
import io.airbyte.workers.temporal.activities.GetWebhookConfigInput
import io.airbyte.workers.temporal.activities.GetWebhookConfigOutput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.GetMaxAttemptOutput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverInput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverOutput
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpStatus
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.joda.time.DateTimeZone
import org.openapitools.client.infrastructure.ClientException
import org.quartz.CronExpression
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.text.ParseException
import java.time.DateTimeException
import java.time.Duration
import java.util.Date
import java.util.List
import java.util.Map
import java.util.Optional
import java.util.Set
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import kotlin.math.max

/**
 * ConfigFetchActivityImpl.
 */
@Singleton
class ConfigFetchActivityImpl
  @VisibleForTesting
  constructor(
    private val airbyteApiClient: AirbyteApiClient,
    @param:Value("\${airbyte.worker.sync.max-attempts}") private val syncJobMaxAttempts: Int,
    @param:Named("currentSecondsSupplier") private val currentSecondsSupplier: Supplier<Long>,
    private val featureFlagClient: FeatureFlagClient,
    private val scheduleJitterHelper: ScheduleJitterHelper,
    private val ffContextMapper: InputFeatureFlagContextMapper,
  ) : ConfigFetchActivity {
    @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
    override fun getTimeToWait(input: ScheduleRetrieverInput): ScheduleRetrieverOutput {
      try {
        ApmTraceUtils.addTagsToTrace(Map.of<String?, UUID?>(CONNECTION_ID_KEY, input.connectionId))
        val connectionIdRequestBody = ConnectionIdRequestBody(input.connectionId!!)
        val connectionRead = airbyteApiClient.connectionApi.getConnection(connectionIdRequestBody)
        val workspaceId = airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(connectionIdRequestBody).workspaceId
        val timeToWait = getTimeToWaitFromScheduleType(connectionRead, input.connectionId!!, workspaceId)
        val timeToWaitWithSchedulingJitter =
          applyJitterRules(timeToWait, input.connectionId!!, connectionRead.scheduleType, workspaceId)
        return ScheduleRetrieverOutput(timeToWaitWithSchedulingJitter)
      } catch (e: ClientException) {
        if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
          throw e
        }
        throw RetryableException(e)
      } catch (e: IOException) {
        throw RetryableException(e)
      }
    }

    override fun getConnectionContext(input: GetConnectionContextInput): GetConnectionContextOutput {
      val connectionIdRequestBody = ConnectionIdRequestBody(input.connectionId)
      val apiContext: ConnectionContextRead
      try {
        apiContext = airbyteApiClient.connectionApi.getConnectionContext(connectionIdRequestBody)
      } catch (e: Exception) {
        throw RetryableException(e)
      }
      val domainContext = apiContext.toInternal()

      return GetConnectionContextOutput(domainContext)
    }

    override fun getLoadShedBackoff(input: GetLoadShedBackoffInput): GetLoadShedBackoffOutput {
      val ffContext = ffContextMapper.map(input.connectionContext)
      val backoffMins = featureFlagClient.intVariation(LoadShedSchedulerBackoffMinutes, ffContext)

      // Ensure no negative backoff.
      if (backoffMins < 0) {
        return GetLoadShedBackoffOutput(Duration.ZERO)
      }

      // Ensure we don't wait longer than an hour so we can minimize manual restarting of workflows and
      // risk of overshedding.
      if (backoffMins > 60) {
        return GetLoadShedBackoffOutput(Duration.ofMinutes(60))
      }

      return GetLoadShedBackoffOutput(Duration.ofMinutes(backoffMins.toLong()))
    }

    private fun applyJitterRules(
      timeToWait: Duration,
      connectionId: UUID,
      scheduleType: ConnectionScheduleType?,
      workspaceId: UUID,
    ): Duration? {
      if (featureFlagClient.boolVariation(
          AddSchedulingJitter,
          Multi(
            List.of<Context?>(
              Workspace(workspaceId),
              Connection(connectionId),
            ),
          ),
        )
      ) {
        return scheduleJitterHelper.addJitterBasedOnWaitTime(timeToWait, scheduleType)
      } else {
        return addSchedulingNoiseForAllowListedWorkspace(timeToWait, scheduleType, workspaceId)
      }
    }

    /**
     * Get time to wait from new schedule. This method consumes the `scheduleType` and `scheduleData`
     * fields.
     *
     * @param connectionRead connection read
     * @param connectionId connection id
     * @return time to wait
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    private fun getTimeToWaitFromScheduleType(
      connectionRead: ConnectionRead,
      connectionId: UUID,
      workspaceId: UUID,
    ): Duration {
      if (connectionRead.scheduleType == ConnectionScheduleType.MANUAL || connectionRead.status != ConnectionStatus.ACTIVE) {
        // Manual syncs wait for their first run
        return Duration.ofDays((100 * 365).toLong())
      }

      val previousJobOptional =
        airbyteApiClient.jobsApi.getLastReplicationJobWithCancel(ConnectionIdRequestBody(connectionId))

      if (connectionRead.scheduleType == ConnectionScheduleType.BASIC) {
        if (previousJobOptional.job == null) {
          // Basic schedules don't wait for their first run.
          return Duration.ZERO
        }
        val prevRunStart = previousJobOptional.job!!.createdAt
        val nextRunStart = prevRunStart + getIntervalInSecond(connectionRead.scheduleData!!.basicSchedule!!)
        val timeToWait =
          Duration.ofSeconds(
            max(0, nextRunStart - currentSecondsSupplier.get()!!),
          )
        return timeToWait
      } else { // connectionRead.getScheduleType() == ConnectionScheduleType.CRON
        val scheduleCron = connectionRead.scheduleData!!.cron
        val timeZone = DateTimeZone.forID(scheduleCron!!.cronTimeZone).toTimeZone()
        try {
          val cronExpression = CronExpression(scheduleCron.cronExpression)
          cronExpression.setTimeZone(timeZone)
          if (featureFlagClient.boolVariation(
              UseNewCronScheduleCalculation,
              Multi(
                List.of<Context?>(
                  Workspace(workspaceId),
                  Connection(connectionId),
                ),
              ),
            )
          ) {
            return CronSchedulingHelper.getNextRuntimeBasedOnPreviousJobAndSchedule(
              currentSecondsSupplier,
              previousJobOptional.job,
              cronExpression,
            )
          } else {
            // Ensure that at least a minimum interval -- one minute -- passes between executions. This prevents
            // us from multiple executions for the same scheduled time, since cron only has a 1-minute
            // resolution.
            val earliestNextRun: Long =
              max(
                currentSecondsSupplier.get()!! * MS_PER_SECOND,
                (
                  if (previousJobOptional.job != null) {
                    if (previousJobOptional.job!!.startedAt != null) {
                      previousJobOptional.job!!.startedAt!! + MIN_CRON_INTERVAL_SECONDS
                    } else {
                      (
                        previousJobOptional.job!!.createdAt +
                          MIN_CRON_INTERVAL_SECONDS
                      )
                    }
                  } else {
                    currentSecondsSupplier.get()
                  }
                )!! * MS_PER_SECOND,
              )
            val nextRunStart = cronExpression.getNextValidTimeAfter(Date(earliestNextRun))
            return Duration.ofSeconds(
              max(0, nextRunStart.getTime() / MS_PER_SECOND - currentSecondsSupplier.get()!!),
            )
          }
        } catch (e: ParseException) {
          throw DateTimeException(e.message).initCause(e) as DateTimeException
        }
      }
    }

    private fun addSchedulingNoiseForAllowListedWorkspace(
      timeToWait: Duration,
      scheduleType: ConnectionScheduleType?,
      workspaceId: UUID?,
    ): Duration? {
      if (!SCHEDULING_NOISE_WORKSPACE_IDS.contains(workspaceId)) {
        // Only apply to a specific set of workspaces.
        return timeToWait
      }
      if (scheduleType != ConnectionScheduleType.CRON) {
        // Only apply noise to cron connections.
        return timeToWait
      }

      // We really do want to add some scheduling noise for this connection.
      val minutesToWait = (Math.random() * SCHEDULING_NOISE_CONSTANT).toLong()
      log.debug("Adding {} minutes noise to wait", minutesToWait)
      // Note: we add an extra second to make the unit tests pass in case `minutesToWait` was 0.
      return timeToWait.plusMinutes(minutesToWait).plusSeconds(1)
    }

    @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
    override fun getMaxAttempt(): GetMaxAttemptOutput = GetMaxAttemptOutput(syncJobMaxAttempts)

    override fun isWorkspaceTombstone(connectionId: UUID): Boolean {
      try {
        val workspaceRead =
          airbyteApiClient.workspaceApi.getWorkspaceByConnectionIdWithTombstone(ConnectionIdRequestBody(connectionId))
        return workspaceRead.tombstone == true
      } catch (e: ClientException) {
        if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
          throw e
        }
        throw RetryableException(e)
      } catch (e: IOException) {
        log.warn("Fail to get the workspace.", e)
        return false
      }
    }

    override fun getWebhookConfig(input: GetWebhookConfigInput): GetWebhookConfigOutput {
      try {
        val jobWebhookConfig: JobWebhookConfig =
          Jsons.deserialize(
            airbyteApiClient.jobsApi.getWebhookConfig(GetWebhookConfigRequest(input.jobId)).value,
            JobWebhookConfig::class.java,
          )
        return GetWebhookConfigOutput(jobWebhookConfig.getOperationSequence(), jobWebhookConfig.getWebhookOperationConfigs())
      } catch (e: Exception) {
        log.warn("Fail to get the webhook config.", e)
        throw RuntimeException(e)
      }
    }

    override fun getSourceId(connectionId: UUID): Optional<UUID> {
      try {
        val requestBody =
          ConnectionIdRequestBody(connectionId)
        val connectionRead = airbyteApiClient.connectionApi.getConnection(requestBody)
        return Optional.ofNullable<UUID?>(connectionRead.sourceId)
      } catch (e: ClientException) {
        if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
          throw e
        }
        throw RetryableException(e)
      } catch (e: IOException) {
        log.info("Encountered an error fetching the connection's Source ID: ", e)
        return Optional.empty<UUID?>()
      }
    }

    override fun getSourceConfig(sourceId: UUID): JsonNode {
      try {
        return airbyteApiClient.sourceApi.getSource(SourceIdRequestBody(sourceId)).connectionConfiguration
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
    }

    override fun getStatus(connectionId: UUID): Optional<ConnectionStatus> {
      try {
        val requestBody =
          ConnectionIdRequestBody(connectionId)
        val connectionRead = airbyteApiClient.connectionApi.getConnection(requestBody)
        return Optional.ofNullable<ConnectionStatus?>(connectionRead.status)
      } catch (e: ClientException) {
        if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
          throw e
        }
        throw RetryableException(e)
      } catch (e: IOException) {
        log.info("Encountered an error fetching the connection's status: ", e)
        return Optional.empty<ConnectionStatus?>()
      }
    }

    private fun getIntervalInSecond(schedule: ConnectionScheduleDataBasicSchedule): Long = getSecondsInUnit(schedule.timeUnit) * schedule.units

    private fun getSecondsInUnit(timeUnitEnum: ConnectionScheduleDataBasicSchedule.TimeUnit): Long {
      when (timeUnitEnum) {
        ConnectionScheduleDataBasicSchedule.TimeUnit.MINUTES -> return TimeUnit.MINUTES.toSeconds(1)
        ConnectionScheduleDataBasicSchedule.TimeUnit.HOURS -> return TimeUnit.HOURS.toSeconds(1)
        ConnectionScheduleDataBasicSchedule.TimeUnit.DAYS -> return TimeUnit.DAYS.toSeconds(1)
        ConnectionScheduleDataBasicSchedule.TimeUnit.WEEKS -> return TimeUnit.DAYS.toSeconds(1) * 7
        ConnectionScheduleDataBasicSchedule.TimeUnit.MONTHS -> return TimeUnit.DAYS.toSeconds(1) * 30
        else -> throw RuntimeException("Unhandled TimeUnitEnum: " + timeUnitEnum)
      }
    }

    companion object {
      private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

      private const val MS_PER_SECOND = 1000L
      private const val MIN_CRON_INTERVAL_SECONDS: Long = 60
      private val SCHEDULING_NOISE_WORKSPACE_IDS: MutableSet<UUID?> =
        Set.of<UUID?>( // Testing
          UUID.fromString("0ace5e1f-4787-43df-8919-456f5f4d03d1"),
          UUID.fromString("20810d92-41a4-4cfd-85db-fb50e77cf36b"), // Prod
          UUID.fromString("226edbc1-4a9c-4401-95a9-90435d667d9d"),
        )
      private const val SCHEDULING_NOISE_CONSTANT: Long = 15
    }
  }
