package io.airbyte.commons.temporal.scheduling

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.EXISTING_PROPERTY,
  property = "type",
)
@JsonSubTypes(
  JsonSubTypes.Type(value = CheckCommandInput::class, name = ConnectorCommandInput.CHECK),
  JsonSubTypes.Type(value = DiscoverCommandInput::class, name = ConnectorCommandInput.DISCOVER),
  JsonSubTypes.Type(value = SpecCommandInput::class, name = ConnectorCommandInput.SPEC),
)
sealed interface ConnectorCommandInput {
  companion object {
    const val CHECK = "check"
    const val DISCOVER = "discover"
    const val SPEC = "spec"
  }

  val type: String
}

@JsonDeserialize(builder = CheckCommandInput.Builder::class)
data class CheckCommandInput(val input: CheckConnectionInput) : ConnectorCommandInput {
  override val type: String = ConnectorCommandInput.CHECK

  // This is duplicated of io.airbyte.workers.model.CheckConnectionInput to avoid dependency hell
  @JsonDeserialize(builder = CheckConnectionInput.Builder::class)
  data class CheckConnectionInput(
    val jobRunConfig: JobRunConfig,
    val integrationLauncherConfig: IntegrationLauncherConfig,
    val checkConnectionInput: StandardCheckConnectionInput,
  ) {
    class Builder
      @JvmOverloads
      constructor(
        var jobRunConfig: JobRunConfig? = null,
        var integrationLauncherConfig: IntegrationLauncherConfig? = null,
        var checkConnectionInput: StandardCheckConnectionInput? = null,
      ) {
        fun jobRunConfig(jobRunConfig: JobRunConfig) = apply { this.jobRunConfig = jobRunConfig }

        fun integrationLauncherConfig(integrationLauncherConfig: IntegrationLauncherConfig) =
          apply {
            this.integrationLauncherConfig = integrationLauncherConfig
          }

        fun checkConnectionInput(checkConnectionInput: StandardCheckConnectionInput) = apply { this.checkConnectionInput = checkConnectionInput }

        fun build() =
          CheckConnectionInput(
            jobRunConfig = jobRunConfig ?: throw IllegalArgumentException("jobRunConfig must be specified"),
            integrationLauncherConfig = integrationLauncherConfig ?: throw IllegalArgumentException("integrationLauncherConfig must be specified"),
            checkConnectionInput = checkConnectionInput ?: throw IllegalArgumentException("checkConnectionInput must be specified"),
          )
      }
  }

  class Builder
    @JvmOverloads
    constructor(var input: CheckConnectionInput? = null) {
      fun input(input: CheckConnectionInput) = apply { this.input = input }

      fun build() = CheckCommandInput(input = input ?: throw IllegalArgumentException("input must be specified"))
    }
}

@JsonDeserialize(builder = DiscoverCommandInput.Builder::class)
data class DiscoverCommandInput(val input: DiscoverCatalogInput) : ConnectorCommandInput {
  override val type: String = ConnectorCommandInput.DISCOVER

  // This is duplicated of io.airbyte.workers.model.DiscoverCatalogInput to avoid dependency hell
  @JsonDeserialize(builder = DiscoverCatalogInput.Builder::class)
  data class DiscoverCatalogInput(
    val jobRunConfig: JobRunConfig,
    val integrationLauncherConfig: IntegrationLauncherConfig,
    val discoverCatalogInput: StandardDiscoverCatalogInput,
  ) {
    class Builder
      @JvmOverloads
      constructor(
        var jobRunConfig: JobRunConfig? = null,
        var integrationLauncherConfig: IntegrationLauncherConfig? = null,
        var discoverCatalogInput: StandardDiscoverCatalogInput? = null,
      ) {
        fun jobRunConfig(jobRunConfig: JobRunConfig) = apply { this.jobRunConfig = jobRunConfig }

        fun integrationLauncherConfig(integrationLauncherConfig: IntegrationLauncherConfig) =
          apply {
            this.integrationLauncherConfig = integrationLauncherConfig
          }

        fun discoverCatalogInput(discoverCatalogInput: StandardDiscoverCatalogInput) = apply { this.discoverCatalogInput = discoverCatalogInput }

        fun build() =
          DiscoverCatalogInput(
            jobRunConfig = jobRunConfig ?: throw IllegalArgumentException("jobRunConfig must be specified"),
            integrationLauncherConfig = integrationLauncherConfig ?: throw IllegalArgumentException("integrationLauncherConfig must be specified"),
            discoverCatalogInput = discoverCatalogInput ?: throw IllegalArgumentException("discoverCatalogInput must be specified"),
          )
      }
  }

  class Builder
    @JvmOverloads
    constructor(var input: DiscoverCatalogInput? = null) {
      fun input(input: DiscoverCatalogInput) = apply { this.input = input }

      fun build() =
        DiscoverCommandInput(
          input = input ?: throw IllegalArgumentException("input must be specified"),
        )
    }
}

@JsonDeserialize(builder = SpecCommandInput.Builder::class)
data class SpecCommandInput(val input: SpecInput) : ConnectorCommandInput {
  override val type: String = ConnectorCommandInput.SPEC

  // This is duplicated of io.airbyte.workers.model.SpecInput to avoid dependency hell
  @JsonDeserialize(builder = SpecInput.Builder::class)
  data class SpecInput(
    val jobRunConfig: JobRunConfig,
    val integrationLauncherConfig: IntegrationLauncherConfig,
  ) {
    class Builder
      @JvmOverloads
      constructor(
        var jobRunConfig: JobRunConfig? = null,
        var integrationLauncherConfig: IntegrationLauncherConfig? = null,
      ) {
        fun jobRunConfig(jobRunConfig: JobRunConfig) = apply { this.jobRunConfig = jobRunConfig }

        fun integrationLauncherConfig(integrationLauncherConfig: IntegrationLauncherConfig) =
          apply {
            this.integrationLauncherConfig = integrationLauncherConfig
          }

        fun build() =
          SpecInput(
            jobRunConfig = jobRunConfig ?: throw IllegalArgumentException("jobRunConfig must be specified"),
            integrationLauncherConfig = integrationLauncherConfig ?: throw IllegalArgumentException("integrationLauncherConfig must be specified"),
          )
      }
  }

  class Builder
    @JvmOverloads
    constructor(var input: SpecInput? = null) {
      fun input(input: SpecInput) = apply { this.input = input }

      fun build() =
        SpecCommandInput(
          input = input ?: throw IllegalArgumentException("input must be specified"),
        )
    }
}

@WorkflowInterface
interface ConnectorCommandWorkflow {
  @WorkflowMethod
  fun run(input: ConnectorCommandInput): ConnectorJobOutput

  @SignalMethod
  fun checkTerminalStatus()
}
