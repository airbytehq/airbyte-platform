/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputStart
import io.micronaut.configuration.picocli.PicocliRunner
import jakarta.inject.Inject
import org.slf4j.LoggerFactory
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.util.UUID

val objectMapper =
  jacksonObjectMapper().apply {
    enable(SerializationFeature.INDENT_OUTPUT)
  }

@Command(
  name = "connector-rollout",
  mixinStandardHelpOptions = true,
  version = ["1.0"],
  description = ["Manages the rollout of the connector using Temporal workflows"],
)
class ConnectorRolloutCLI : Runnable {
  @Inject
  private lateinit var connectorRolloutClient: ConnectorRolloutClient
  private val availableCommands = RolloutCommand.entries.joinToString(", ") { it.command }

  @Parameters(
    index = "0",
    description = ["The command to execute. See --help for available commands."],
  )
  private lateinit var command: String

  @Option(
    names = ["-d", "--docker-repository"],
    description = ["Name of the connector"],
    required = true,
  )
  private lateinit var dockerRepository: String

  @Option(
    names = ["-i", "--docker-image-tag"],
    description = ["Version of the connector"],
    required = true,
  )
  private lateinit var dockerImageTag: String

  @Option(
    names = ["-a", "--actor-definition-id"],
    description = ["Unique identifier of the connector"],
    required = true,
  )
  private lateinit var actorDefinitionId: UUID

  @Option(
    names = ["-r", "--rollout-id"],
    description = ["Id of the rollout"],
    required = false,
  )
  private var rolloutId: UUID? = null

  @Option(
    names = ["-c", "--actor-ids"],
    description = ["List of actor IDs (required for `rollout` command)"],
    split = ",",
    required = false,
  )
  private var actorIds: List<UUID>? = null

  companion object {
    private val log = LoggerFactory.getLogger(ConnectorRolloutCLI::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
      PicocliRunner.run(ConnectorRolloutCLI::class.java, *args)
    }
  }

  override fun run() {
    if (command.isBlank()) {
      println("Error: Please specify a command. Available commands: $availableCommands")
      return
    }

    val rolloutCommand = RolloutCommand.fromString(command)
    log.info("CLI Running command: $rolloutCommand")

    when (rolloutCommand) {
      RolloutCommand.START -> {
        val startInput =
          ConnectorRolloutActivityInputStart(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
          )
        startWorkflow(connectorRolloutClient, startInput)
      }
      RolloutCommand.FIND -> {
        val findInput =
          ConnectorRolloutActivityInputFind(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
          )
        findRollout(connectorRolloutClient, findInput)
      }
      RolloutCommand.GET -> {
        val getInput =
          ConnectorRolloutActivityInputGet(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
          )
        getRollout(connectorRolloutClient, getInput)
      }
      RolloutCommand.ROLLOUT -> {
        val rolloutInput =
          ConnectorRolloutActivityInputRollout(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
            actorIds!!,
          )
        doRollout(connectorRolloutClient, rolloutInput)
      }
      RolloutCommand.PROMOTE -> {
        val finalizeInput =
          ConnectorRolloutActivityInputFinalize(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
            ConnectorRolloutFinalState.SUCCEEDED,
            null,
            null,
          )
        finalizeRollout(connectorRolloutClient, finalizeInput)
      }
      RolloutCommand.FAIL -> {
        val finalizeInput =
          ConnectorRolloutActivityInputFinalize(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
            ConnectorRolloutFinalState.FAILED_ROLLED_BACK,
            null,
            null,
          )
        finalizeRollout(connectorRolloutClient, finalizeInput)
      }
      RolloutCommand.CANCEL -> {
        val finalizeInput =
          ConnectorRolloutActivityInputFinalize(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
            ConnectorRolloutFinalState.CANCELED_ROLLED_BACK,
            null,
            null,
          )
        finalizeRollout(connectorRolloutClient, finalizeInput)
      }
      else -> {
        log.info("CLI unknown command $command")
        CommandLine.usage(this, System.out)
      }
    }
  }

  private fun startWorkflow(
    client: ConnectorRolloutClient,
    input: ConnectorRolloutActivityInputStart,
  ) {
    logFormatted("CLI.startWorkflow with input", input)
    logFormatted("CLI Rollout workflows status", client.startWorkflow(input))
  }

  private fun findRollout(
    client: ConnectorRolloutClient,
    input: ConnectorRolloutActivityInputFind,
  ) {
    logFormatted("CLI Rollout workflows status", client.findRollout(input))
  }

  private fun getRollout(
    client: ConnectorRolloutClient,
    input: ConnectorRolloutActivityInputGet,
  ) {
    logFormatted("CLI Rollout workflows status", client.getRollout(input))
  }

  private fun doRollout(
    client: ConnectorRolloutClient,
    input: ConnectorRolloutActivityInputRollout,
  ) {
    if (actorIds.isNullOrEmpty()) {
      throw RuntimeException("CLI Error - please provide a comma-separated list of actor IDs using the --actor-ids option.")
    }
    logFormatted("CLI Rollout workflows status", client.doRollout(input))
  }

  private fun finalizeRollout(
    client: ConnectorRolloutClient,
    input: ConnectorRolloutActivityInputFinalize,
  ) {
    val rolloutOutput = client.finalizeRollout(input)
    logFormatted("CLI Rollout workflow status", rolloutOutput)
    val workflowResult = client.getWorkflowResult(input)
    log.info("CLI Workflow result: $workflowResult")
  }

  private fun <T> logFormatted(
    message: String,
    data: T,
  ) {
    val jsonOutput = objectMapper.writeValueAsString(data)
    log.info("$message:\n$jsonOutput")
  }
}
