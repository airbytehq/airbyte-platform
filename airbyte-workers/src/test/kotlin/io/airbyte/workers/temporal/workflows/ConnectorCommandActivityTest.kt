package io.airbyte.workers.temporal.workflows

import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.temporal.scheduling.SpecCommandInput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
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
  lateinit var metricClient: MetricClient

  @BeforeEach
  fun setup() {
    checkCommand = mockk(relaxed = true)
    discoverCommand = mockk(relaxed = true)
    specCommand = mockk(relaxed = true)
    metricClient = mockk(relaxed = true)
    activity = ConnectorCommandActivityImpl(checkCommand, discoverCommand, specCommand, metricClient)
  }

  @Test
  fun `test instrumentation`() {
    val input = getCheckInput()
    activity.startCommand(input, null)

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
    assertEquals("startCommand", commandStepTags[MetricTags.COMMAND_STEP])
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
        activity.cancelCommand(input, "something")
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
    activity.cancelCommand(checkInput, "check cancel test")
    verify { checkCommand.cancel("check cancel test") }

    val discoverInput = getDiscoverInput()
    activity.cancelCommand(discoverInput, "discover cancel test")
    verify { discoverCommand.cancel("discover cancel test") }

    val specInput = getSpecInput()
    activity.cancelCommand(specInput, "spec cancel test")
    verify { specCommand.cancel("spec cancel test") }
  }

  @Test
  fun `test getOutput dispatching works`() {
    val checkInput = getCheckInput()
    activity.getCommandOutput(checkInput, "check getOutput test")
    verify { checkCommand.getOutput("check getOutput test") }

    val discoverInput = getDiscoverInput()
    activity.getCommandOutput(discoverInput, "discover getOutput test")
    verify { discoverCommand.getOutput("discover getOutput test") }

    val specInput = getSpecInput()
    activity.getCommandOutput(specInput, "spec getOutput test")
    verify { specCommand.getOutput("spec getOutput test") }
  }

  @Test
  fun `test isTerminal dispatching works`() {
    val checkInput = getCheckInput()
    activity.isCommandTerminal(checkInput, "check isTerminal test")
    verify { checkCommand.isTerminal("check isTerminal test") }

    val discoverInput = getDiscoverInput()
    activity.isCommandTerminal(discoverInput, "discover isTerminal test")
    verify { discoverCommand.isTerminal("discover isTerminal test") }

    val specInput = getSpecInput()
    activity.isCommandTerminal(specInput, "spec isTerminal test")
    verify { specCommand.isTerminal("spec isTerminal test") }
  }

  @Test
  fun `test start dispatching works`() {
    val signalPayload = "signal"
    val checkInput = getCheckInput()
    activity.startCommand(checkInput, signalPayload)
    verify {
      checkCommand.start(
        CheckConnectionInput(checkInput.input.jobRunConfig, checkInput.input.integrationLauncherConfig, checkInput.input.checkConnectionInput),
        signalPayload,
      )
    }

    val discoverInput = getDiscoverInput()
    activity.startCommand(discoverInput, signalPayload)
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
    activity.startCommand(specInput, signalPayload)
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
}
