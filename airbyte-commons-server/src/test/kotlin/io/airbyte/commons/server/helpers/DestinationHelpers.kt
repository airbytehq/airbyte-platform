/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.SupportState
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.data.services.shared.DestinationConnectionWithCount
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID

object DestinationHelpers {
  @get:Throws(IOException::class)
  val testDestinationJson: JsonNode
    get() {
      val path =
        Paths.get(
          DestinationHelpers::class.java
            .getClassLoader()
            .getResource("json/TestImplementation.json")
            .getPath(),
        )

      return deserialize(Files.readString(path))
    }

  @Throws(IOException::class)
  fun generateDestination(
    destinationDefinitionId: UUID?,
    tombstone: Boolean,
  ): DestinationConnection = generateDestination(destinationDefinitionId, "my default dest name", tombstone, null)

  @Throws(IOException::class)
  fun generateDestination(
    destinationDefinitionId: UUID?,
    resourceRequirements: ScopedResourceRequirements?,
  ): DestinationConnection = generateDestination(destinationDefinitionId, "my default dest name", false, resourceRequirements)

  @JvmOverloads
  @Throws(IOException::class)
  fun generateDestination(
    destinationDefinitionId: UUID?,
    name: String? = "my default dest name",
    tombstone: Boolean = false,
    resourceRequirements: ScopedResourceRequirements? = null,
  ): DestinationConnection {
    val workspaceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()

    val implementationJson: JsonNode = testDestinationJson

    return DestinationConnection()
      .withName(name)
      .withWorkspaceId(workspaceId)
      .withDestinationDefinitionId(destinationDefinitionId)
      .withDestinationId(destinationId)
      .withConfiguration(implementationJson)
      .withTombstone(tombstone)
      .withResourceRequirements(resourceRequirements)
  }

  fun generateDestinationWithCount(destination: DestinationConnection): DestinationConnectionWithCount =
    DestinationConnectionWithCount(destination, "destination-definition", 0, null, mapOf(), true)

  val resourceRequirementsForDestination: ScopedResourceRequirements?
    get() =
      ScopedResourceRequirements()
        .withDefault(ResourceRequirements().withCpuRequest("2").withMemoryRequest("2"))

  fun getDestinationRead(
    destination: DestinationConnection,
    standardDestinationDefinition: StandardDestinationDefinition,
  ): DestinationRead {
    // sets reasonable defaults for isVersionOverrideApplied and supportState, use below method instead
    // if you want to override them.
    return getDestinationRead(destination, standardDestinationDefinition, false, true, SupportState.SUPPORTED, null)
  }

  fun getDestinationRead(
    destination: DestinationConnection,
    standardDestinationDefinition: StandardDestinationDefinition,
    isVersionOverrideApplied: Boolean,
    isEntitled: Boolean,
    supportState: SupportState?,
    resourceAllocation: io.airbyte.api.model.generated.ScopedResourceRequirements?,
  ): DestinationRead =
    DestinationRead()
      .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
      .workspaceId(destination.getWorkspaceId())
      .destinationDefinitionId(destination.getDestinationDefinitionId())
      .destinationId(destination.getDestinationId())
      .connectionConfiguration(destination.getConfiguration())
      .name(destination.getName())
      .destinationName(standardDestinationDefinition.getName())
      .icon(standardDestinationDefinition.getIconUrl())
      .isVersionOverrideApplied(isVersionOverrideApplied)
      .isEntitled(isEntitled)
      .supportState(supportState)
      .resourceAllocation(resourceAllocation)
}
