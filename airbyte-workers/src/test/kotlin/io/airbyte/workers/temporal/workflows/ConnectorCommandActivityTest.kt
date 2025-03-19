/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.workflows

import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.temporal.scheduling.SpecCommandInput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.DiscoverCommand
import io.airbyte.workers.commands.SpecCommand
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ConnectorCommandActivityTest {
  lateinit var activity: ConnectorCommandActivity
  lateinit var checkCommand: CheckCommand
  lateinit var discoverCommand: DiscoverCommand
  lateinit var specCommand: SpecCommand
  lateinit var activityExecutionContextProvider: ActivityExecutionContextProvider
  lateinit var metricClient: MetricClient

  @BeforeEach
  fun setup() {
    checkCommand = mockk(relaxed = true)
    discoverCommand = mockk(relaxed = true)
    specCommand = mockk(relaxed = true)
    activityExecutionContextProvider = mockk(relaxed = true)
    metricClient = mockk(relaxed = true)
    activity = ConnectorCommandActivityImpl(checkCommand, discoverCommand, specCommand, activityExecutionContextProvider, metricClient)
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

    val discoverInput = getDiscoverInput()
    activity.cancelCommand(getActivityInput(input = discoverInput, id = "discover cancel test"))
    verify { discoverCommand.cancel("discover cancel test") }

    val specInput = getSpecInput()
    activity.cancelCommand(getActivityInput(input = specInput, id = "spec cancel test"))
    verify { specCommand.cancel("spec cancel test") }
  }

  @Test
  fun `test getOutput dispatching works`() {
    val checkInput = getCheckInput()
    activity.getCommandOutput(getActivityInput(input = checkInput, id = "check getOutput test"))
    verify { checkCommand.getOutput("check getOutput test") }

    val discoverInput = getDiscoverInput()
    activity.getCommandOutput(getActivityInput(input = discoverInput, id = "discover getOutput test"))
    verify { discoverCommand.getOutput("discover getOutput test") }

    val specInput = getSpecInput()
    activity.getCommandOutput(getActivityInput(input = specInput, id = "spec getOutput test"))
    verify { specCommand.getOutput("spec getOutput test") }
  }

  @Test
  fun `test isTerminal dispatching works`() {
    val checkInput = getCheckInput()
    activity.isCommandTerminal(getActivityInput(input = checkInput, id = "check isTerminal test"))
    verify { checkCommand.isTerminal("check isTerminal test") }

    val discoverInput = getDiscoverInput()
    activity.isCommandTerminal(getActivityInput(input = discoverInput, id = "discover isTerminal test"))
    verify { discoverCommand.isTerminal("discover isTerminal test") }

    val specInput = getSpecInput()
    activity.isCommandTerminal(getActivityInput(input = specInput, id = "spec isTerminal test"))
    verify { specCommand.isTerminal("spec isTerminal test") }
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

    val specInput = getSpecInput()
    activity.startCommand(getActivityInput(input = specInput, signalPayload = signalPayload))
    verify {
      specCommand.start(
        SpecInput(specInput.input.jobRunConfig, specInput.input.integrationLauncherConfig),
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

  private fun getDiscoverInput() =
    DiscoverCommandInput(
      input =
        DiscoverCommandInput.DiscoverCatalogInput(
          jobRunConfig = JobRunConfig(),
          integrationLauncherConfig = IntegrationLauncherConfig(),
          discoverCatalogInput = StandardDiscoverCatalogInput(),
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

  private fun getActivityInput(
    input: ConnectorCommandInput,
    signalPayload: String? = null,
    id: String? = null,
    startTimeInMillis: Long = 0,
  ) = ConnectorCommandActivityInput(input = input, signalPayload = signalPayload, id = id, startTimeInMillis = startTimeInMillis)
}
