/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutListRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutManualFinalizeRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutManualRolloutRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutManualStartRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutReadRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutStateTerminal
import io.airbyte.api.client.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorRolloutFinalState
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.configuration.picocli.PicocliRunner
import jakarta.inject.Inject
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.util.UUID

private val logger = KotlinLogging.logger {}

val objectMapper =
  jacksonObjectMapper().apply {
    enable(SerializationFeature.INDENT_OUTPUT)
    registerModule(JavaTimeModule())
  }

@Command(
  name = "connector-rollout",
  mixinStandardHelpOptions = true,
  version = ["1.0"],
  description = ["Manages the rollout of the connector using Temporal workflows"],
)
class ConnectorRolloutCLI : Runnable {
  @Inject
  private lateinit var airbyteApiClient: AirbyteApiClient
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
    names = ["--actor-ids"],
    description = ["List of actor IDs (required for `rollout` command)"],
    split = ",",
    required = false,
  )
  private var actorIds: List<UUID>? = null

  companion object {
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

    val rolloutClient = airbyteApiClient.connectorRolloutApi

    val rolloutCommand = RolloutCommand.fromString(command)
    logger.info { "CLI Running command: $rolloutCommand" }

    when (rolloutCommand) {
      RolloutCommand.START -> {
        val startInput =
          ConnectorRolloutManualStartRequestBody(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            UUID(0, 0),
            ConnectorRolloutStrategy.MANUAL,
          )
        startWorkflow(rolloutClient, startInput)
      }
      RolloutCommand.FIND -> {
        val findInput =
          ConnectorRolloutListRequestBody(
            dockerImageTag,
            actorDefinitionId,
          )
        findRollout(rolloutClient, findInput)
      }
      RolloutCommand.GET -> {
        val getInput = ConnectorRolloutReadRequestBody(rolloutId!!)
        getRollout(rolloutClient, getInput)
      }
      RolloutCommand.ROLLOUT -> {
        val rolloutInput =
          ConnectorRolloutManualRolloutRequestBody(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            UUID(0, 0),
            rolloutId!!,
            actorIds!!,
          )
        doRollout(rolloutClient, rolloutInput)
      }
      RolloutCommand.PROMOTE -> {
        val finalizeInput =
          ConnectorRolloutManualFinalizeRequestBody(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
            UUID(0, 0),
            ConnectorRolloutStateTerminal.valueOf(ConnectorRolloutFinalState.SUCCEEDED.toString()),
            null,
            null,
          )
        finalizeRollout(rolloutClient, finalizeInput)
      }
      RolloutCommand.FAIL -> {
        val finalizeInput =
          ConnectorRolloutManualFinalizeRequestBody(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
            UUID(0, 0),
            ConnectorRolloutStateTerminal.FAILED_ROLLED_BACK,
            null,
            null,
          )
        finalizeRollout(rolloutClient, finalizeInput)
      }
      RolloutCommand.CANCEL -> {
        val finalizeInput =
          ConnectorRolloutManualFinalizeRequestBody(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            rolloutId!!,
            UUID(0, 0),
            ConnectorRolloutStateTerminal.CANCELED,
            null,
            null,
          )
        finalizeRollout(rolloutClient, finalizeInput)
      }
      else -> {
        logger.info { "CLI unknown command $command" }
        CommandLine.usage(this, System.out)
      }
    }
  }

  private fun startWorkflow(
    client: ConnectorRolloutApi,
    input: ConnectorRolloutManualStartRequestBody,
  ) {
    logFormatted("CLI.startWorkflow using client", client)
    logFormatted("CLI.startWorkflow with input", input)
    logFormatted("CLI Rollout workflows status", client.manualStartConnectorRollout(input))
  }

  private fun findRollout(
    client: ConnectorRolloutApi,
    input: ConnectorRolloutListRequestBody,
  ) {
    logFormatted("CLI Rollout workflows status", client.getConnectorRolloutsList(input))
  }

  private fun getRollout(
    client: ConnectorRolloutApi,
    input: ConnectorRolloutReadRequestBody,
  ) {
    logFormatted("CLI Rollout workflows status", client.getConnectorRolloutById(input))
  }

  private fun doRollout(
    client: ConnectorRolloutApi,
    input: ConnectorRolloutManualRolloutRequestBody,
  ) {
    if (actorIds.isNullOrEmpty()) {
      throw RuntimeException("CLI Error - please provide a comma-separated list of actor IDs using the --actor-ids option.")
    }
    logFormatted("CLI Rollout workflows status", client.manualDoConnectorRollout(input))
  }

  private fun finalizeRollout(
    client: ConnectorRolloutApi,
    input: ConnectorRolloutManualFinalizeRequestBody,
  ) {
    logFormatted("CLI Rollout finalized", client.manualFinalizeConnectorRollout(input))
  }

  private fun <T> logFormatted(
    message: String,
    data: T,
  ) {
    val jsonOutput = objectMapper.writeValueAsString(data)
    logger.info { "$message:\n$jsonOutput" }
  }
}
