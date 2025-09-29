/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.input

import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectionContext
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Destination
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.Source
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.Workspace
import io.airbyte.featureflag.merge
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput

/**
 * Extension functions to turn config models into feature flag contexts for use
 * in environments without DI.
 *
 * For DI-friendly and more testable Singleton version, see [InputFeatureFlagContextMapper]
 */
fun ActorContext.toFeatureFlagContext(): Context {
  val childContexts =
    buildList {
      workspaceId?.let { add(Workspace(it)) }
      organizationId?.let { add(Organization(it)) }

      when (actorType) {
        ActorType.SOURCE -> {
          actorId?.let { add(Source(it)) }
          actorDefinitionId?.let { add(SourceDefinition(it)) }
        }

        ActorType.DESTINATION -> {
          actorId?.let { add(Destination(it)) }
          actorDefinitionId?.let { add(DestinationDefinition(it)) }
        }

        null -> {}
      }
    }

  return Multi.orEmpty(childContexts)
}

fun IntegrationLauncherConfig.toFeatureFlagContext(): Context {
  val childContexts =
    buildList {
      connectionId?.let { add(Connection(it)) }
      workspaceId?.let { add(Workspace(it)) }
    }

  return Multi.orEmpty(childContexts)
}

fun ConnectionContext.toFeatureFlagContext(): Context {
  val childContexts =
    buildList {
      connectionId?.let { add(Connection(it)) }
      workspaceId?.let { add(Workspace(it)) }
      organizationId?.let { add(Organization(it)) }
      sourceId?.let { add(Source(it)) }
      sourceDefinitionId?.let { add(SourceDefinition(it)) }
      destinationId?.let { add(Destination(it)) }
      destinationDefinitionId?.let { add(DestinationDefinition(it)) }
    }

  return Multi.orEmpty(childContexts)
}

fun CheckConnectionInput.toFeatureFlagContext(): Context {
  val actorContext = this.checkConnectionInput.actorContext?.toFeatureFlagContext() ?: Empty
  val launcherContext = this.launcherConfig.toFeatureFlagContext()
  return actorContext.merge(launcherContext)
}

fun DiscoverCatalogInput.toFeatureFlagContext(): Context {
  val actorContext = this.discoverCatalogInput.actorContext?.toFeatureFlagContext() ?: Empty
  val launcherContext = this.launcherConfig.toFeatureFlagContext()
  return actorContext.merge(launcherContext)
}

fun SpecInput.toFeatureFlagContext(): Context = this.launcherConfig.toFeatureFlagContext()

fun ReplicationInput.toFeatureFlagContext(): Context = this.connectionContext?.toFeatureFlagContext() ?: Empty
