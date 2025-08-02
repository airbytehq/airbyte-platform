/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ReleaseStage
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.Companion.hasAlphaOrBetaVersion
import io.airbyte.config.persistence.versionoverrides.ConfigurationDefinitionVersionOverrideProvider
import io.airbyte.config.persistence.versionoverrides.DefinitionVersionOverrideProvider
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.io.IOException
import java.util.Map
import java.util.Optional
import java.util.UUID

internal class ActorDefinitionVersionHelperTest {
  private lateinit var mConfigOverrideProvider: DefinitionVersionOverrideProvider
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper

  @BeforeEach
  @Throws(ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
  fun setup() {
    mConfigOverrideProvider = Mockito.mock(ConfigurationDefinitionVersionOverrideProvider::class.java)
    Mockito
      .`when`(
        mConfigOverrideProvider.getOverride(
          org.mockito.kotlin.any<UUID>(),
          org.mockito.kotlin.any<UUID>(),
          org.mockito.kotlin.any<UUID>(),
        ),
      ).thenReturn(Optional.empty())

    actorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(DEFAULT_VERSION_ID))
      .thenReturn(DEFAULT_VERSION)
    actorDefinitionVersionHelper = ActorDefinitionVersionHelper(actorDefinitionService, mConfigOverrideProvider)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetSourceVersion() {
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID)

    val versionWithOverrideStatus =
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, ACTOR_ID)
    Assertions.assertEquals(DEFAULT_VERSION, versionWithOverrideStatus.actorDefinitionVersion)
    Assertions.assertFalse(versionWithOverrideStatus.isOverrideApplied)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetSourceVersionWithConfigOverride(isOverrideApplied: Boolean) {
    Mockito
      .`when`(
        mConfigOverrideProvider.getOverride(
          ACTOR_DEFINITION_ID,
          WORKSPACE_ID,
          ACTOR_ID,
        ),
      ).thenReturn(
        Optional.of(
          ActorDefinitionVersionWithOverrideStatus(
            OVERRIDDEN_VERSION,
            isOverrideApplied,
          ),
        ),
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID)

    val versionWithOverrideStatus =
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, ACTOR_ID)
    Assertions.assertEquals(OVERRIDDEN_VERSION, versionWithOverrideStatus.actorDefinitionVersion)
    Assertions.assertEquals(isOverrideApplied, versionWithOverrideStatus.isOverrideApplied)

    Mockito.verify<DefinitionVersionOverrideProvider?>(mConfigOverrideProvider).getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetSourceVersionForWorkspace() {
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID)

    val actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID)
    Assertions.assertEquals(DEFAULT_VERSION, actual)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetSourceVersionForWorkspaceWithConfigOverride() {
    Mockito
      .`when`(
        mConfigOverrideProvider.getOverride(
          ACTOR_DEFINITION_ID,
          WORKSPACE_ID,
          null,
        ),
      ).thenReturn(Optional.of(ActorDefinitionVersionWithOverrideStatus(OVERRIDDEN_VERSION, true)))

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID)

    val actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID)
    Assertions.assertEquals(OVERRIDDEN_VERSION, actual)

    Mockito.verify<DefinitionVersionOverrideProvider?>(mConfigOverrideProvider).getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, null)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetDestinationVersion() {
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID)

    val versionWithOverrideStatus =
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID, ACTOR_ID)
    Assertions.assertEquals(DEFAULT_VERSION, versionWithOverrideStatus.actorDefinitionVersion)
    Assertions.assertFalse(versionWithOverrideStatus.isOverrideApplied)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetDestinationVersionWithConfigOverride() {
    Mockito
      .`when`(
        mConfigOverrideProvider.getOverride(
          ACTOR_DEFINITION_ID,
          WORKSPACE_ID,
          ACTOR_ID,
        ),
      ).thenReturn(Optional.of(ActorDefinitionVersionWithOverrideStatus(OVERRIDDEN_VERSION, true)))

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID)

    val versionWithOverrideStatus =
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID, ACTOR_ID)
    Assertions.assertEquals(OVERRIDDEN_VERSION, versionWithOverrideStatus.actorDefinitionVersion)
    Assertions.assertTrue(versionWithOverrideStatus.isOverrideApplied)

    Mockito.verify<DefinitionVersionOverrideProvider?>(mConfigOverrideProvider).getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetDestinationVersionForWorkspace() {
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID)

    val actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID)
    Assertions.assertEquals(DEFAULT_VERSION, actual)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetDestinationVersionForWorkspaceWithConfigOverride() {
    Mockito
      .`when`(
        mConfigOverrideProvider.getOverride(
          ACTOR_DEFINITION_ID,
          WORKSPACE_ID,
          null,
        ),
      ).thenReturn(Optional.of(ActorDefinitionVersionWithOverrideStatus(OVERRIDDEN_VERSION, true)))

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID)

    val actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID)
    Assertions.assertEquals(OVERRIDDEN_VERSION, actual)

    Mockito.verify<DefinitionVersionOverrideProvider?>(mConfigOverrideProvider).getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, null)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetDefaultSourceVersion() {
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(ACTOR_DEFINITION_VERSION_ID)

    Mockito.`when`<ActorDefinitionVersion?>(actorDefinitionService!!.getActorDefinitionVersion(ACTOR_DEFINITION_VERSION_ID)).thenReturn(
      DEFAULT_VERSION,
    )

    val result = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID)
    Assertions.assertEquals(DEFAULT_VERSION, result)
  }

  @Test
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun testGetDefaultDestinationVersion() {
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(ACTOR_DEFINITION_VERSION_ID)

    Mockito.`when`<ActorDefinitionVersion?>(actorDefinitionService!!.getActorDefinitionVersion(ACTOR_DEFINITION_VERSION_ID)).thenReturn(
      DEFAULT_VERSION,
    )

    val result = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID)
    Assertions.assertEquals(DEFAULT_VERSION, result)
  }

  @Test
  fun testGetDefaultVersionWithNoDefaultThrows() {
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)

    val exception =
      Assertions.assertThrows<RuntimeException>(
        RuntimeException::class.java,
        Executable { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID) },
      )
    Assertions.assertTrue(exception.message!!.contains("Default version for source is not set"))
  }

  @Test
  fun testGetDefaultDestinationVersionWithNoDefaultThrows() {
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)

    val exception =
      Assertions.assertThrows<RuntimeException>(
        RuntimeException::class.java,
        Executable { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID) },
      )
    Assertions.assertTrue(exception.message!!.contains("Default version for destination is not set"))
  }

  @ParameterizedTest
  @CsvSource("alpha,generally_available,true", "beta,generally_available,true", "generally_available,generally_available,false", "alpha,beta,true")
  fun testHasAlphaOrBeta(
    sourceReleaseStageStr: String?,
    destinationReleaseStageStr: String?,
    expected: Boolean,
  ) {
    val sourceDefVersion = ActorDefinitionVersion().withReleaseStage(ReleaseStage.fromValue(sourceReleaseStageStr))
    val destDefVersion = ActorDefinitionVersion().withReleaseStage(ReleaseStage.fromValue(destinationReleaseStageStr))
    Assertions.assertEquals(expected, hasAlphaOrBetaVersion(listOf(sourceDefVersion, destDefVersion)))
  }

  companion object {
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private val ACTOR_DEFINITION_VERSION_ID: UUID = UUID.randomUUID()
    private val ACTOR_ID: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val DOCKER_REPOSITORY = "airbyte/source-test"
    private const val DOCKER_IMAGE_TAG = "0.1.0"
    private const val DOCKER_IMAGE_TAG_2 = "0.2.0"
    private val SPEC: ConnectorSpecification? =
      ConnectorSpecification()
        .withConnectionSpecification(
          jsonNode<MutableMap<String?, String?>?>(
            Map.of<String?, String?>(
              "key",
              "value",
            ),
          ),
        )
    private val SPEC_2: ConnectorSpecification? =
      ConnectorSpecification()
        .withConnectionSpecification(
          jsonNode<MutableMap<String?, String?>?>(
            Map.of<String?, String?>(
              "key",
              "value",
              "key2",
              "value2",
            ),
          ),
        )

    private val DEFAULT_VERSION_ID: UUID = UUID.randomUUID()

    private val DEFAULT_VERSION: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(DEFAULT_VERSION_ID)
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC)
    private val OVERRIDDEN_VERSION: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG_2)
        .withSpec(SPEC_2)
  }
}
