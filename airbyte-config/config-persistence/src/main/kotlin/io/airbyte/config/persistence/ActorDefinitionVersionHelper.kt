/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ReleaseStage
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.versionoverrides.DefinitionVersionOverrideProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Helper class for retrieving the actor definition version to use when running a connector. This
 * should be used when a specific actor or workspace is present, rather than accessing the fields
 * directly on the definitions.
 */
@Singleton
class ActorDefinitionVersionHelper(
  private val actorDefinitionService: ActorDefinitionService,
  @param:Named("configurationVersionOverrideProvider") private val configOverrideProvider: DefinitionVersionOverrideProvider,
) {
  /**
   * A wrapper class for returning the actor definition version and whether an override was applied.
   *
   * @param actorDefinitionVersion - actor definition version to use
   * @param isOverrideApplied - true if the version is the result of an override being applied,
   * otherwise false
   */
  @JvmRecord
  data class ActorDefinitionVersionWithOverrideStatus(
    @JvmField val actorDefinitionVersion: ActorDefinitionVersion,
    @JvmField val isOverrideApplied: Boolean,
  )

  init {
    log.info(
      "ActorDefinitionVersionHelper initialized with override provider: {}",
      configOverrideProvider.javaClass.simpleName,
    )
  }

  @Throws(IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun getDefaultSourceVersion(sourceDefinition: StandardSourceDefinition): ActorDefinitionVersion {
    val versionId =
      sourceDefinition.defaultVersionId
        ?: throw RuntimeException(
          String.format(
            "Default version for source is not set (Definition ID: %s)",
            sourceDefinition.sourceDefinitionId,
          ),
        )

    return actorDefinitionService.getActorDefinitionVersion(versionId)
  }

  @Throws(IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun getDefaultDestinationVersion(destinationDefinition: StandardDestinationDefinition): ActorDefinitionVersion {
    val versionId =
      destinationDefinition.defaultVersionId
        ?: throw RuntimeException(
          String.format(
            "Default version for destination is not set (Definition ID: %s)",
            destinationDefinition.destinationDefinitionId,
          ),
        )

    return actorDefinitionService.getActorDefinitionVersion(versionId)
  }

  /**
   * Getting versions from a list of definitions.
   *
   * @param shownSourceDefs Definitions to get versions for.
   * @param workspaceId UUID of the workspace, not currently used
   *
   * @return Map of ids to definition versions
   */
  fun getSourceVersions(
    shownSourceDefs: List<StandardSourceDefinition>,
    workspaceId: UUID,
  ): Map<UUID, ActorDefinitionVersion?> {
    try {
      val overrides =
        configOverrideProvider
          .getOverrides(
            shownSourceDefs
              .stream()
              .map { obj: StandardSourceDefinition -> obj.sourceDefinitionId }
              .toList(),
            workspaceId,
          ) // Map to DefinitionId, DefinitionVersion
          .stream()
          .collect(
            Collectors.toMap(
              { defWithOverride: ActorDefinitionVersionWithOverrideStatus -> defWithOverride.actorDefinitionVersion.actorDefinitionId },
              { defWithOverride: ActorDefinitionVersionWithOverrideStatus -> defWithOverride.actorDefinitionVersion },
            ),
          )

      // Get all the actorDefinitionVersions for definitions that do not have an override.
      val sourceVersions =
        actorDefinitionService
          .getActorDefinitionVersions(
            shownSourceDefs
              .stream() // Filter out definitions that have a version override
              .filter { version: StandardSourceDefinition -> overrides[version.sourceDefinitionId] == null }
              .map { obj: StandardSourceDefinition -> obj.defaultVersionId }
              .toList(),
          ).stream()
          .collect(
            Collectors.toMap(
              { obj: ActorDefinitionVersion? -> obj!!.actorDefinitionId },
              Function.identity(),
            ),
          )

      // Merge overrides and non-overrides together
      sourceVersions.putAll(overrides)

      return sourceVersions
    } catch (e: IOException) {
      log.error(e.localizedMessage)
      throw RuntimeException(e)
    }
  }

  /**
   * Getting versions from a list of definitions.
   *
   * @param shownDestinationDefs Definitions to get versions for.
   * @param workspaceId UUID of the workspace, not currently used
   *
   * @return Map of ids to definition versions
   */
  fun getDestinationVersions(
    shownDestinationDefs: List<StandardDestinationDefinition>,
    workspaceId: UUID,
  ): Map<UUID, ActorDefinitionVersion?> {
    try {
      val overrides =
        configOverrideProvider
          .getOverrides(
            shownDestinationDefs
              .stream()
              .map { obj: StandardDestinationDefinition -> obj.destinationDefinitionId }
              .toList(),
            workspaceId,
          ) // Map to DefinitionId, DefinitionVersion
          .stream()
          .collect(
            Collectors.toMap(
              { defWithOverride: ActorDefinitionVersionWithOverrideStatus -> defWithOverride.actorDefinitionVersion.actorDefinitionId },
              { defWithOverride: ActorDefinitionVersionWithOverrideStatus -> defWithOverride.actorDefinitionVersion },
            ),
          )

      // Get all the actorDefinitionVersions for definitions that do not have an override.
      val destinationVersions =
        actorDefinitionService
          .getActorDefinitionVersions(
            shownDestinationDefs
              .stream() // Filter out definitions that have a version override
              .filter { version: StandardDestinationDefinition -> overrides[version.destinationDefinitionId] == null }
              .map { obj: StandardDestinationDefinition -> obj.defaultVersionId }
              .toList(),
          ).stream()
          .collect(
            Collectors.toMap(
              { obj: ActorDefinitionVersion? -> obj!!.actorDefinitionId },
              Function.identity(),
            ),
          )

      // Merge overrides and non-overrides together
      destinationVersions.putAll(overrides)

      return destinationVersions
    } catch (e: IOException) {
      log.error(e.localizedMessage)
      throw RuntimeException(e)
    }
  }

  /**
   * Get the actor definition version to use for a source, and whether an override was applied.
   *
   * @param sourceDefinition source definition
   * @param workspaceId workspace id
   * @param actorId source id
   * @return actor definition version with override status
   */
  @Throws(IOException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun getSourceVersionWithOverrideStatus(
    sourceDefinition: StandardSourceDefinition,
    workspaceId: UUID,
    @Nullable actorId: UUID?,
  ): ActorDefinitionVersionWithOverrideStatus {
    val defaultVersion = getDefaultSourceVersion(sourceDefinition)

    val versionOverride =
      configOverrideProvider.getOverride(
        sourceDefinition.sourceDefinitionId,
        workspaceId,
        actorId,
      )

    return versionOverride.orElse(ActorDefinitionVersionWithOverrideStatus(defaultVersion, false))
  }

  /**
   * Get the actor definition version to use for a source.
   *
   * @param sourceDefinition source definition
   * @param workspaceId workspace id
   * @param actorId source id
   * @return actor definition version
   */
  @Throws(IOException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun getSourceVersion(
    sourceDefinition: StandardSourceDefinition,
    workspaceId: UUID,
    @Nullable actorId: UUID?,
  ): ActorDefinitionVersion = getSourceVersionWithOverrideStatus(sourceDefinition, workspaceId, actorId).actorDefinitionVersion

  /**
   * Get the actor definition version to use for sources in a given workspace.
   *
   * @param sourceDefinition source definition
   * @param workspaceId workspace id
   * @return actor definition version
   */
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun getSourceVersion(
    sourceDefinition: StandardSourceDefinition,
    workspaceId: UUID,
  ): ActorDefinitionVersion = getSourceVersion(sourceDefinition, workspaceId, null)

  /**
   * Get the actor definition version to use for a destination, and whether an override was applied.
   *
   * @param destinationDefinition destination definition
   * @param workspaceId workspace id
   * @param actorId destination id
   * @return actor definition version with override status
   */
  @Throws(IOException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun getDestinationVersionWithOverrideStatus(
    destinationDefinition: StandardDestinationDefinition,
    workspaceId: UUID,
    @Nullable actorId: UUID?,
  ): ActorDefinitionVersionWithOverrideStatus {
    val defaultVersion = getDefaultDestinationVersion(destinationDefinition)

    val versionOverride =
      configOverrideProvider.getOverride(
        destinationDefinition.destinationDefinitionId,
        workspaceId,
        actorId,
      )

    return versionOverride.orElse(ActorDefinitionVersionWithOverrideStatus(defaultVersion, false))
  }

  /**
   * Get the actor definition version to use for a destination.
   *
   * @param destinationDefinition destination definition
   * @param workspaceId workspace id
   * @param actorId destination id
   * @return actor definition version
   */
  @Throws(IOException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun getDestinationVersion(
    destinationDefinition: StandardDestinationDefinition,
    workspaceId: UUID,
    @Nullable actorId: UUID?,
  ): ActorDefinitionVersion = getDestinationVersionWithOverrideStatus(destinationDefinition, workspaceId, actorId).actorDefinitionVersion

  /**
   * Get the actor definition version to use for destinations in a given workspace.
   *
   * @param destinationDefinition destination definition
   * @param workspaceId workspace id
   * @return actor definition version
   */
  @Throws(IOException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun getDestinationVersion(
    destinationDefinition: StandardDestinationDefinition,
    workspaceId: UUID,
  ): ActorDefinitionVersion = getDestinationVersion(destinationDefinition, workspaceId, null)

  companion object {
    private val log = KotlinLogging.logger {}

    /**
     * Get the docker image name (docker_repository:docker_image_tag) for a given actor definition
     * version.
     *
     * @param actorDefinitionVersion actor definition version
     * @return docker image name
     */
    @JvmStatic
    fun getDockerImageName(actorDefinitionVersion: ActorDefinitionVersion): String =
      String.format("%s:%s", actorDefinitionVersion.dockerRepository, actorDefinitionVersion.dockerImageTag)

    /**
     * Helper method to share eligibility logic for free connector program.
     *
     * @param actorDefinitionVersions List of versions that should be checked for alpha/beta status
     * @return true if any of the provided versions is in alpha or beta
     */
    @JvmStatic
    fun hasAlphaOrBetaVersion(actorDefinitionVersions: List<ActorDefinitionVersion>): Boolean =
      actorDefinitionVersions
        .stream()
        .anyMatch { version: ActorDefinitionVersion ->
          version.releaseStage == ReleaseStage.ALPHA || version.releaseStage == ReleaseStage.BETA
        }
  }
}
