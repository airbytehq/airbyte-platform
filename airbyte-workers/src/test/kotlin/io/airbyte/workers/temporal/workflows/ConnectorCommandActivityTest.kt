/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.workflows

import io.airbyte.commons.converters.log
import io.airbyte.commons.temporal.scheduling.CheckCommandApiInput
import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.DiscoverCommandApiInput
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.commons.temporal.scheduling.SpecCommandInput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.CheckCommandThroughApi
import io.airbyte.workers.commands.DiscoverCommand
import io.airbyte.workers.commands.DiscoverCommandV2
import io.airbyte.workers.commands.ReplicationCommand
import io.airbyte.workers.commands.SpecCommand
import io.airbyte.workers.models.CheckConnectionApiInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.DiscoverSourceApiInput
import io.airbyte.workers.models.ReplicationApiInput
import io.airbyte.workers.models.SpecInput
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ConnectorCommandActivityTest {
  lateinit var activity: ConnectorCommandActivity
  lateinit var checkCommand: CheckCommand
  lateinit var checkCommandThroughApi: CheckCommandThroughApi
  lateinit var discoverCommand: DiscoverCommand
  lateinit var discoverCommandV2: DiscoverCommandV2
  lateinit var specCommand: SpecCommand
  lateinit var replicationCommand: ReplicationCommand
  lateinit var activityExecutionContextProvider: ActivityExecutionContextProvider
  lateinit var metricClient: MetricClient

  @BeforeEach
  fun setup() {
    checkCommand = mockk(relaxed = true)
    discoverCommand = mockk(relaxed = true)
    specCommand = mockk(relaxed = true)
    checkCommandThroughApi = mockk(relaxed = true)
    replicationCommand = mockk(relaxed = true)
    discoverCommandV2 = mockk(relaxed = true)
    activityExecutionContextProvider = mockk(relaxed = true)
    metricClient = mockk(relaxed = true)
    activity =
      ConnectorCommandActivityImpl(
        checkCommand,
        checkCommandThroughApi,
        discoverCommand,
        discoverCommandV2,
        specCommand,
        replicationCommand,
        activityExecutionContextProvider,
        metricClient,
      )
  }

  @Test
  fun `test instrumentation`() {
    every { activityExecutionContextProvider.get() } returns
      mockk(relaxed = true) {
        every { info } returns
          mockk(relaxed = true) {
            every { activityType } returns "StartCommand"
          }
      }
    val input = getCheckInput()
    activity.startCommand(getActivityInput(input = input, signalPayload = null))

    val commandStepTags = mutableMapOf<String, String>()
    verify {
      metricClient.count(
        OssMetricsRegistry.COMMAND_STEP,
        1,
        *varargAll {
          commandStepTags[it.key] = it.value
          true
        },
      )
    }

    val commandStepDurationTags = mutableMapOf<String, String>()
    verify {
      metricClient.distribution(
        OssMetricsRegistry.COMMAND_STEP_DURATION,
        any(),
        *varargAll {
          commandStepDurationTags[it.key] = it.value
          true
        },
      )
    }

    assertEquals(commandStepTags, commandStepDurationTags)
    assertEquals("StartCommand", commandStepTags[MetricTags.COMMAND_STEP])
    assertEquals(input.type, commandStepTags[MetricTags.COMMAND])
    assertEquals(MetricTags.SUCCESS, commandStepTags[MetricTags.STATUS])
  }

  @Test
  fun `test instrumentation with failures`() {
    val input = getCheckInput()
    val exception = Exception("Error")
    every { checkCommand.cancel(any()) } throws exception

    val actualException =
      assertThrows<Exception> {
        activity.cancelCommand(getActivityInput(input = input, id = "something"))
      }
    assertEquals(exception, actualException)

    verify {
      metricClient.count(
        OssMetricsRegistry.COMMAND_STEP,
        1,
        *varargAny {
          it.key == MetricTags.STATUS && it.value == MetricTags.FAILURE
        },
      )
    }
  }

  @Test
  fun `test cancel dispatching works`() {
    val checkInput = getCheckInput()
    activity.cancelCommand(getActivityInput(input = checkInput, id = "check cancel test"))
    verify { checkCommand.cancel("check cancel test") }

    val checkApiInput = getCheckCommandApiInput()
    activity.cancelCommand(getActivityInput(input = checkApiInput, id = "check api cancel test"))
    verify { checkCommandThroughApi.cancel("check api cancel test") }

    val discoverInput = getDiscoverInput()
    activity.cancelCommand(getActivityInput(input = discoverInput, id = "discover cancel test"))
    verify { discoverCommand.cancel("discover cancel test") }

    val discoverApiInput = getDiscoverCommandApiInput()
    activity.cancelCommand(getActivityInput(input = discoverApiInput, id = "discover api cancel test"))
    verify { discoverCommandV2.cancel("discover api cancel test") }

    val specInput = getSpecInput()
    activity.cancelCommand(getActivityInput(input = specInput, id = "spec cancel test"))
    verify { specCommand.cancel("spec cancel test") }

    val replicationInput = getReplicationInput()
    activity.cancelCommand(getActivityInput(input = replicationInput, id = "replication cancel test"))
    verify { replicationCommand.cancel("replication cancel test") }
  }

  @Test
  fun `test getOutput dispatching works`() {
    val checkInput = getCheckInput()
    activity.getCommandOutput(getActivityInput(input = checkInput, id = "check getOutput test"))
    verify { checkCommand.getOutput("check getOutput test") }

    val checkApiInput = getCheckCommandApiInput()
    activity.getCommandOutput(getActivityInput(input = checkApiInput, id = "check api getOutput test"))
    verify { checkCommandThroughApi.getOutput("check api getOutput test") }

    val discoverInput = getDiscoverInput()
    activity.getCommandOutput(getActivityInput(input = discoverInput, id = "discover getOutput test"))
    verify { discoverCommand.getOutput("discover getOutput test") }

    val discoverApiInput = getDiscoverCommandApiInput()
    activity.getCommandOutput(getActivityInput(input = discoverApiInput, id = "discover api cancel test"))
    verify { discoverCommandV2.getOutput("discover api cancel test") }

    val specInput = getSpecInput()
    activity.getCommandOutput(getActivityInput(input = specInput, id = "spec getOutput test"))
    verify { specCommand.getOutput("spec getOutput test") }

    val replicationInput = getReplicationInput()
    activity.getCommandOutput(getActivityInput(input = replicationInput, id = "replication getOutput test"))
    verify { replicationCommand.getOutput("replication getOutput test") }
  }

  @Test
  fun `test isTerminal dispatching works`() {
    val checkInput = getCheckInput()
    activity.isCommandTerminal(getActivityInput(input = checkInput, id = "check isTerminal test"))
    verify { checkCommand.isTerminal("check isTerminal test") }

    val checkApiInput = getCheckCommandApiInput()
    activity.isCommandTerminal(getActivityInput(input = checkApiInput, id = "check api isTerminal test"))
    verify { checkCommandThroughApi.isTerminal("check api isTerminal test") }

    val discoverInput = getDiscoverInput()
    activity.isCommandTerminal(getActivityInput(input = discoverInput, id = "discover isTerminal test"))
    verify { discoverCommand.isTerminal("discover isTerminal test") }

    val discoverApiInput = getDiscoverCommandApiInput()
    activity.isCommandTerminal(getActivityInput(input = discoverApiInput, id = "discover api cancel test"))
    verify { discoverCommandV2.isTerminal("discover api cancel test") }

    val specInput = getSpecInput()
    activity.isCommandTerminal(getActivityInput(input = specInput, id = "spec isTerminal test"))
    verify { specCommand.isTerminal("spec isTerminal test") }

    val replicationInput = getReplicationInput()
    activity.isCommandTerminal(getActivityInput(input = replicationInput, id = "replication isTerminal test"))
    verify { replicationCommand.isTerminal("replication isTerminal test") }
  }

  @Test
  fun `test start dispatching works`() {
    val signalPayload = "signal"
    val checkInput = getCheckInput()
    activity.startCommand(getActivityInput(input = checkInput, signalPayload = signalPayload))
    verify {
      checkCommand.start(
        CheckConnectionInput(checkInput.input.jobRunConfig, checkInput.input.integrationLauncherConfig, checkInput.input.checkConnectionInput),
        signalPayload,
      )
    }

    val checkApiInput = getCheckCommandApiInput()
    activity.startCommand(getActivityInput(input = checkApiInput, signalPayload = signalPayload))
    log.error { CheckConnectionApiInput(checkApiInput.input.actorId, checkApiInput.input.jobId, checkApiInput.input.attemptId) }
    verify {
      checkCommandThroughApi.start(
        CheckConnectionApiInput(checkApiInput.input.actorId, checkApiInput.input.jobId, checkApiInput.input.attemptId),
        signalPayload,
      )
    }

    val discoverInput = getDiscoverInput()
    activity.startCommand(getActivityInput(input = discoverInput, signalPayload = signalPayload))
    verify {
      discoverCommand.start(
        DiscoverCatalogInput(
          discoverInput.input.jobRunConfig,
          discoverInput.input.integrationLauncherConfig,
          discoverInput.input.discoverCatalogInput,
        ),
        signalPayload,
      )
    }

    val discoverApiInput = getDiscoverCommandApiInput()
    activity.startCommand(getActivityInput(input = discoverApiInput, signalPayload = signalPayload))
    verify {
      discoverCommandV2.start(
        DiscoverSourceApiInput(discoverApiInput.input.actorId, discoverApiInput.input.jobId, discoverApiInput.input.attemptNumber),
        signalPayload,
      )
    }

    val specInput = getSpecInput()
    activity.startCommand(getActivityInput(input = specInput, signalPayload = signalPayload))
    verify {
      specCommand.start(
        SpecInput(specInput.input.jobRunConfig, specInput.input.integrationLauncherConfig),
        signalPayload,
      )
    }

    val replicationInput = getReplicationInput()
    activity.startCommand(getActivityInput(input = replicationInput, signalPayload = signalPayload))
    verify {
      replicationCommand.start(
        ReplicationApiInput(
          replicationInput.input.connectionId,
          replicationInput.input.jobId,
          replicationInput.input.attemptId,
          replicationInput.input.appliedCatalogDiff,
        ),
        signalPayload,
      )
    }
  }

  private fun getCheckInput() =
    CheckCommandInput(
      input =
        CheckCommandInput.CheckConnectionInput(
          jobRunConfig = JobRunConfig(),
          integrationLauncherConfig = IntegrationLauncherConfig(),
          checkConnectionInput = StandardCheckConnectionInput(),
        ),
    )

  private fun getCheckCommandApiInput() =
    CheckCommandApiInput(
      input =
        CheckCommandApiInput.CheckConnectionApiInput(
          actorId = UUID.randomUUID(),
          jobId = "jobId",
          attemptId = 1337L,
        ),
    )

  private fun getDiscoverInput() =
    DiscoverCommandInput(
      input =
        DiscoverCommandInput.DiscoverCatalogInput(
          jobRunConfig = JobRunConfig(),
          integrationLauncherConfig = IntegrationLauncherConfig(),
          discoverCatalogInput = StandardDiscoverCatalogInput(),
        ),
    )

  private fun getDiscoverCommandApiInput() =
    DiscoverCommandApiInput(
      input =
        DiscoverCommandApiInput.DiscoverApiInput(
          actorId = UUID.randomUUID(),
          jobId = "jobId",
          attemptNumber = 1337L,
        ),
    )

  private fun getSpecInput() =
    SpecCommandInput(
      input =
        SpecCommandInput.SpecInput(
          jobRunConfig = JobRunConfig(),
          integrationLauncherConfig = IntegrationLauncherConfig(),
        ),
    )

  private fun getReplicationInput() =
    ReplicationCommandApiInput(
      input =
        ReplicationCommandApiInput.ReplicationApiInput(
          connectionId = UUID.randomUUID(),
          jobId = "jobId",
          attemptId = 1337L,
          appliedCatalogDiff = null,
        ),
    )

  private fun getActivityInput(
    input: ConnectorCommandInput,
    signalPayload: String? = null,
    id: String? = null,
    startTimeInMillis: Long = 0,
  ) = ConnectorCommandActivityInput(input = input, signalPayload = signalPayload, id = id, startTimeInMillis = startTimeInMillis)
}
