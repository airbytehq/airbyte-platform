/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SupportState
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.services.shared.SourceConnectionWithCount
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID

object SourceHelpers {
  @Throws(IOException::class)
  fun generateSource(
    sourceDefinitionId: UUID?,
    tombstone: Boolean,
  ): SourceConnection = generateSource(sourceDefinitionId, "my default source name", tombstone, null)

  @Throws(IOException::class)
  fun generateSource(
    sourceDefinitionId: UUID?,
    resourceRequirements: ScopedResourceRequirements?,
  ): SourceConnection = generateSource(sourceDefinitionId, "my default source name", false, resourceRequirements)

  @JvmOverloads
  @Throws(IOException::class)
  fun generateSource(
    sourceDefinitionId: UUID?,
    name: String? = "my default source name",
    tombstone: Boolean = false,
    resourceRequirements: ScopedResourceRequirements? = null,
  ): SourceConnection {
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()

    val implementationJson: JsonNode = testImplementationJson

    return SourceConnection()
      .withName(name)
      .withWorkspaceId(workspaceId)
      .withSourceDefinitionId(sourceDefinitionId)
      .withSourceId(sourceId)
      .withConfiguration(implementationJson)
      .withTombstone(tombstone)
      .withResourceRequirements(resourceRequirements)
  }

  @get:Throws(IOException::class)
  val testImplementationJson: JsonNode
    get() {
      val path =
        Paths.get(
          SourceHelpers::class.java
            .getClassLoader()
            .getResource("json/TestImplementation.json")
            .getPath(),
        )
      return deserialize(Files.readString(path))
    }

  fun generateSourceWithCount(source: SourceConnection): SourceConnectionWithCount =
    SourceConnectionWithCount(source, "faker", 0, null, mapOf(), true)

  fun getSourceRead(
    source: SourceConnection,
    standardSourceDefinition: StandardSourceDefinition,
  ): SourceRead {
    // sets reasonable defaults for isVersionOverrideApplied and supportState, use below method instead
    // if you want to override them.
    return getSourceRead(source, standardSourceDefinition, false, true, SupportState.SUPPORTED, null)
  }

  fun getSourceRead(
    source: SourceConnection,
    standardSourceDefinition: StandardSourceDefinition,
    isVersionOverrideApplied: Boolean,
    isEntitled: Boolean,
    supportState: SupportState?,
    resourceAllocation: io.airbyte.api.model.generated.ScopedResourceRequirements?,
  ): SourceRead =
    SourceRead()
      .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
      .workspaceId(source.getWorkspaceId())
      .sourceDefinitionId(source.getSourceDefinitionId())
      .sourceId(source.getSourceId())
      .connectionConfiguration(source.getConfiguration())
      .name(source.getName())
      .sourceName(standardSourceDefinition.getName())
      .icon(standardSourceDefinition.getIconUrl())
      .isVersionOverrideApplied(isVersionOverrideApplied)
      .isEntitled(isEntitled)
      .supportState(supportState)
      .resourceAllocation(resourceAllocation)
}
