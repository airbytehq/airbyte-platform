/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.util.UUID

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.EXISTING_PROPERTY,
  property = "type",
)
@JsonSubTypes(
  JsonSubTypes.Type(value = CheckCommandInput::class, name = ConnectorCommandInput.CHECK),
  JsonSubTypes.Type(value = CheckCommandApiInput::class, name = ConnectorCommandInput.CHECK_COMMAND),
  JsonSubTypes.Type(value = DiscoverCommandInput::class, name = ConnectorCommandInput.DISCOVER),
  JsonSubTypes.Type(value = DiscoverCommandApiInput::class, name = ConnectorCommandInput.DISCOVER_COMMAND),
  JsonSubTypes.Type(value = SpecCommandInput::class, name = ConnectorCommandInput.SPEC),
  JsonSubTypes.Type(value = ReplicationCommandApiInput::class, name = ConnectorCommandInput.REPLICATION_COMMAND),
)
sealed interface ConnectorCommandInput {
  companion object {
    const val CHECK = "check"
    const val CHECK_COMMAND = "check_command"
    const val DISCOVER = "discover"
    const val DISCOVER_COMMAND = "discover_command"
    const val SPEC = "spec"
    const val REPLICATION_COMMAND = "replication_command"
  }

  val type: String
}

@JsonDeserialize(builder = CheckCommandInput.Builder::class)
data class CheckCommandInput(
  val input: CheckConnectionInput,
) : ConnectorCommandInput {
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
    constructor(
      var input: CheckConnectionInput? = null,
    ) {
      fun input(input: CheckConnectionInput) = apply { this.input = input }

      fun build() = CheckCommandInput(input = input ?: throw IllegalArgumentException("input must be specified"))
    }
}

@JsonDeserialize(builder = CheckCommandApiInput.Builder::class)
data class CheckCommandApiInput(
  val input: CheckConnectionApiInput,
) : ConnectorCommandInput {
  override val type: String = ConnectorCommandInput.CHECK_COMMAND

  // This is duplicated of io.airbyte.workers.model.CheckConnectionInput to avoid dependency hell
  @JsonDeserialize(builder = CheckConnectionApiInput.Builder::class)
  data class CheckConnectionApiInput(
    val actorId: UUID,
    val jobId: String,
    val attemptId: Long,
  ) {
    class Builder
      @JvmOverloads
      constructor(
        var actorId: UUID? = null,
        var jobId: String? = null,
        var attemptId: Long? = null,
      ) {
        fun actorId(actorId: UUID) = apply { this.actorId = actorId }

        fun jobId(jobId: String) = apply { this.jobId = jobId }

        fun attemptId(attemptId: Long) = apply { this.attemptId = attemptId }

        fun build() =
          CheckConnectionApiInput(
            actorId = actorId ?: throw IllegalArgumentException("actorId must be specified"),
            jobId = jobId ?: throw IllegalArgumentException("jobId must be specified"),
            attemptId = attemptId ?: throw IllegalArgumentException("attemptId must be specified"),
          )
      }
  }

  class Builder
    @JvmOverloads
    constructor(
      var input: CheckConnectionApiInput? = null,
    ) {
      fun input(input: CheckConnectionApiInput) = apply { this.input = input }

      fun build() = CheckCommandApiInput(input = input ?: throw IllegalArgumentException("input must be specified"))
    }
}

@JsonDeserialize(builder = DiscoverCommandApiInput.Builder::class)
data class DiscoverCommandApiInput(
  val input: DiscoverApiInput,
) : ConnectorCommandInput {
  override val type: String = ConnectorCommandInput.DISCOVER_COMMAND

  // This is duplicated of io.airbyte.workers.model.CheckConnectionInput to avoid dependency hell
  @JsonDeserialize(builder = DiscoverApiInput.Builder::class)
  data class DiscoverApiInput(
    val actorId: UUID,
    val jobId: String,
    val attemptNumber: Long,
  ) {
    class Builder
      @JvmOverloads
      constructor(
        var actorId: UUID? = null,
        var jobId: String? = null,
        var attemptNumber: Long? = null,
      ) {
        fun actorId(actorId: UUID) = apply { this.actorId = actorId }

        fun jobId(jobId: String) = apply { this.jobId = jobId }

        fun attemptNumber(attemptNumber: Long) = apply { this.attemptNumber = attemptNumber }

        fun build() =
          DiscoverApiInput(
            actorId = actorId ?: throw IllegalArgumentException("actorId must be specified"),
            jobId = jobId ?: throw IllegalArgumentException("jobId must be specified"),
            attemptNumber = attemptNumber ?: throw IllegalArgumentException("attemptId must be specified"),
          )
      }
  }

  class Builder
    @JvmOverloads
    constructor(
      var input: DiscoverApiInput? = null,
    ) {
      fun input(input: DiscoverApiInput) = apply { this.input = input }

      fun build() = DiscoverCommandApiInput(input = input ?: throw IllegalArgumentException("input must be specified"))
    }
}

@JsonDeserialize(builder = DiscoverCommandInput.Builder::class)
data class DiscoverCommandInput(
  val input: DiscoverCatalogInput,
) : ConnectorCommandInput {
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
    constructor(
      var input: DiscoverCatalogInput? = null,
    ) {
      fun input(input: DiscoverCatalogInput) = apply { this.input = input }

      fun build() =
        DiscoverCommandInput(
          input = input ?: throw IllegalArgumentException("input must be specified"),
        )
    }
}

@JsonDeserialize(builder = SpecCommandInput.Builder::class)
data class SpecCommandInput(
  val input: SpecInput,
) : ConnectorCommandInput {
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
    constructor(
      var input: SpecInput? = null,
    ) {
      fun input(input: SpecInput) = apply { this.input = input }

      fun build() =
        SpecCommandInput(
          input = input ?: throw IllegalArgumentException("input must be specified"),
        )
    }
}

@JsonDeserialize(builder = ReplicationCommandApiInput.Builder::class)
data class ReplicationCommandApiInput(
  val input: ReplicationApiInput,
) : ConnectorCommandInput {
  override val type: String = ConnectorCommandInput.REPLICATION_COMMAND

  @JsonDeserialize(builder = ReplicationApiInput.Builder::class)
  data class ReplicationApiInput(
    val connectionId: UUID,
    val jobId: String,
    val attemptId: Long,
    val appliedCatalogDiff: CatalogDiff?,
  ) {
    class Builder
      @JvmOverloads
      constructor(
        var connectionId: UUID? = null,
        var jobId: String? = null,
        var attemptId: Long? = null,
        var appliedCatalogDiff: CatalogDiff? = null,
      ) {
        fun actorId(connectionId: UUID) = apply { this.connectionId = connectionId }

        fun jobId(jobId: String) = apply { this.jobId = jobId }

        fun attemptId(attemptId: Long) = apply { this.attemptId = attemptId }

        fun appliedCatalogDiff(appliedCatalogDiff: CatalogDiff) = apply { this.appliedCatalogDiff = appliedCatalogDiff }

        fun build() =
          ReplicationApiInput(
            connectionId = connectionId ?: throw IllegalArgumentException("connectionId must be specified"),
            jobId = jobId ?: throw IllegalArgumentException("jobId must be specified"),
            attemptId = attemptId ?: throw IllegalArgumentException("attemptId must be specified"),
            appliedCatalogDiff = appliedCatalogDiff,
          )
      }
  }

  class Builder
    @JvmOverloads
    constructor(
      var input: ReplicationApiInput? = null,
    ) {
      fun input(input: ReplicationApiInput) = apply { this.input = input }

      fun build() = ReplicationCommandApiInput(input = input ?: throw IllegalArgumentException("input must be specified"))
    }
}

@WorkflowInterface
interface ConnectorCommandWorkflow {
  @WorkflowMethod
  fun run(input: ConnectorCommandInput): ConnectorJobOutput

  @SignalMethod
  fun checkTerminalStatus()
}
