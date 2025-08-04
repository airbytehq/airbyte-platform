/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.versionoverrides

import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AllowedHosts
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ReleaseStage
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SuggestedStreams
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.io.IOException
import java.util.Map
import java.util.Optional
import java.util.UUID

internal class ConfigurationDefinitionVersionOverrideProviderTest {
  private lateinit var mWorkspaceService: WorkspaceService
  private lateinit var mActorDefinitionService: ActorDefinitionService
  private lateinit var mScopedConfigurationService: ScopedConfigurationService
  private lateinit var overrideProvider: ConfigurationDefinitionVersionOverrideProvider

  @BeforeEach
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun setup() {
    mWorkspaceService = Mockito.mock(WorkspaceService::class.java)
    mActorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    mScopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    overrideProvider =
      ConfigurationDefinitionVersionOverrideProvider(mWorkspaceService, mActorDefinitionService, mScopedConfigurationService)

    Mockito
      .`when`(mWorkspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withOrganizationId(ORGANIZATION_ID))
  }

  @Test
  fun testGetVersionNoOverride() {
    Mockito
      .`when`(
        mScopedConfigurationService.getScopedConfiguration(
          ConnectorVersionKey,
          ConfigResourceType.ACTOR_DEFINITION,
          ACTOR_DEFINITION_ID,
          Map.of<ConfigScopeType, UUID>(
            ConfigScopeType.ORGANIZATION,
            ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE,
            WORKSPACE_ID,
            ConfigScopeType.ACTOR,
            ACTOR_ID,
          ),
        ),
      ).thenReturn(Optional.empty())

    val optResult: Optional<ActorDefinitionVersionWithOverrideStatus> =
      overrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID)
    Assertions.assertTrue(optResult.isEmpty())

    Mockito.verify(mScopedConfigurationService).getScopedConfiguration(
      ConnectorVersionKey,
      ConfigResourceType.ACTOR_DEFINITION,
      ACTOR_DEFINITION_ID,
      Map.of<ConfigScopeType, UUID>(
        ConfigScopeType.ORGANIZATION,
        ORGANIZATION_ID,
        ConfigScopeType.WORKSPACE,
        WORKSPACE_ID,
        ConfigScopeType.ACTOR,
        ACTOR_ID,
      ),
    )

    Mockito.verifyNoInteractions(mActorDefinitionService)
  }

  @ParameterizedTest
  @ValueSource(strings = ["user", "breaking_change"])
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun testGetVersionWithOverride(originTypeStr: String) {
    val versionId = UUID.randomUUID()
    val versionConfig =
      ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(WORKSPACE_ID)
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withResourceId(ACTOR_DEFINITION_ID)
        .withValue(versionId.toString())
        .withOriginType(ConfigOriginType.fromValue(originTypeStr))

    Mockito
      .`when`(
        mScopedConfigurationService.getScopedConfiguration(
          ConnectorVersionKey,
          ConfigResourceType.ACTOR_DEFINITION,
          ACTOR_DEFINITION_ID,
          Map.of<ConfigScopeType, UUID>(
            ConfigScopeType.ORGANIZATION,
            ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE,
            WORKSPACE_ID,
            ConfigScopeType.ACTOR,
            ACTOR_ID,
          ),
        ),
      ).thenReturn(Optional.of(versionConfig))

    Mockito.`when`(mActorDefinitionService.getActorDefinitionVersion(versionId)).thenReturn(OVERRIDE_VERSION)

    val optResult: Optional<ActorDefinitionVersionWithOverrideStatus> =
      overrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID)

    Assertions.assertTrue(optResult.isPresent())
    Assertions.assertEquals(OVERRIDE_VERSION, optResult.get().actorDefinitionVersion)

    val expectedOverrideStatus = originTypeStr == "user"
    Assertions.assertEquals(expectedOverrideStatus, optResult.get().isOverrideApplied)

    Mockito.verify(mScopedConfigurationService).getScopedConfiguration(
      ConnectorVersionKey,
      ConfigResourceType.ACTOR_DEFINITION,
      ACTOR_DEFINITION_ID,
      Map.of<ConfigScopeType, UUID>(
        ConfigScopeType.ORGANIZATION,
        ORGANIZATION_ID,
        ConfigScopeType.WORKSPACE,
        WORKSPACE_ID,
        ConfigScopeType.ACTOR,
        ACTOR_ID,
      ),
    )
    Mockito.verify(mActorDefinitionService).getActorDefinitionVersion(versionId)

    Mockito.verifyNoMoreInteractions(mScopedConfigurationService, mActorDefinitionService)
  }

  @ParameterizedTest
  @ValueSource(strings = ["user", "breaking_change"])
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun testGetVersionWithOverrideNoActor(originTypeStr: String) {
    val versionId = UUID.randomUUID()
    val versionConfig =
      ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(WORKSPACE_ID)
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withResourceId(ACTOR_DEFINITION_ID)
        .withValue(versionId.toString())
        .withOriginType(ConfigOriginType.fromValue(originTypeStr))

    Mockito
      .`when`(
        mScopedConfigurationService.getScopedConfiguration(
          ConnectorVersionKey,
          ConfigResourceType.ACTOR_DEFINITION,
          ACTOR_DEFINITION_ID,
          Map.of<ConfigScopeType, UUID>(
            ConfigScopeType.ORGANIZATION,
            ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE,
            WORKSPACE_ID,
          ),
        ),
      ).thenReturn(Optional.of(versionConfig))

    Mockito.`when`(mActorDefinitionService.getActorDefinitionVersion(versionId)).thenReturn(OVERRIDE_VERSION)

    val optResult: Optional<ActorDefinitionVersionWithOverrideStatus> =
      overrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, null)

    Assertions.assertTrue(optResult.isPresent())
    Assertions.assertEquals(OVERRIDE_VERSION, optResult.get().actorDefinitionVersion)

    val expectedOverrideStatus = originTypeStr == "user"
    Assertions.assertEquals(expectedOverrideStatus, optResult.get().isOverrideApplied)

    Mockito.verify(mScopedConfigurationService).getScopedConfiguration(
      ConnectorVersionKey,
      ConfigResourceType.ACTOR_DEFINITION,
      ACTOR_DEFINITION_ID,
      Map.of<ConfigScopeType, UUID>(
        ConfigScopeType.ORGANIZATION,
        ORGANIZATION_ID,
        ConfigScopeType.WORKSPACE,
        WORKSPACE_ID,
      ),
    )
    Mockito.verify(mActorDefinitionService).getActorDefinitionVersion(versionId)

    Mockito.verifyNoMoreInteractions(mScopedConfigurationService, mActorDefinitionService)
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun testThrowsIfVersionIdDoesNotExist() {
    val versionId = UUID.randomUUID()
    val versionConfig =
      ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(WORKSPACE_ID)
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withResourceId(ACTOR_DEFINITION_ID)
        .withValue(versionId.toString())

    Mockito
      .`when`(
        mScopedConfigurationService.getScopedConfiguration(
          ConnectorVersionKey,
          ConfigResourceType.ACTOR_DEFINITION,
          ACTOR_DEFINITION_ID,
          Map.of<ConfigScopeType, UUID>(
            ConfigScopeType.ORGANIZATION,
            ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE,
            WORKSPACE_ID,
            ConfigScopeType.ACTOR,
            ACTOR_ID,
          ),
        ),
      ).thenReturn(Optional.of(versionConfig))

    Mockito
      .`when`(mActorDefinitionService.getActorDefinitionVersion(versionId))
      .thenThrow(ConfigNotFoundException(ConfigNotFoundType.ACTOR_DEFINITION_VERSION, versionId))

    Assertions.assertThrows(
      RuntimeException::class.java,
      Executable { overrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID) },
    )

    Mockito.verify(mScopedConfigurationService).getScopedConfiguration(
      ConnectorVersionKey,
      ConfigResourceType.ACTOR_DEFINITION,
      ACTOR_DEFINITION_ID,
      Map.of<ConfigScopeType, UUID>(
        ConfigScopeType.ORGANIZATION,
        ORGANIZATION_ID,
        ConfigScopeType.WORKSPACE,
        WORKSPACE_ID,
        ConfigScopeType.ACTOR,
        ACTOR_ID,
      ),
    )
    Mockito.verify(mActorDefinitionService).getActorDefinitionVersion(versionId)

    Mockito.verifyNoMoreInteractions(mScopedConfigurationService, mActorDefinitionService)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun testBCPinIntegrityMetricsEmitted(withMismatchedVersions: Boolean?) {
    val versionId: UUID = OVERRIDE_VERSION.getVersionId()
    val breakingChangePin =
      ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(WORKSPACE_ID)
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withResourceId(ACTOR_DEFINITION_ID)
        .withValue(versionId.toString())
        .withOriginType(ConfigOriginType.BREAKING_CHANGE)

    Mockito
      .`when`(
        mScopedConfigurationService.getScopedConfiguration(
          ConnectorVersionKey,
          ConfigResourceType.ACTOR_DEFINITION,
          ACTOR_DEFINITION_ID,
          Map.of<ConfigScopeType, UUID>(
            ConfigScopeType.ORGANIZATION,
            ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE,
            WORKSPACE_ID,
            ConfigScopeType.ACTOR,
            ACTOR_ID,
          ),
        ),
      ).thenReturn(Optional.of(breakingChangePin))

    Mockito.`when`(mActorDefinitionService.getActorDefinitionVersion(versionId)).thenReturn(OVERRIDE_VERSION)

    val optResult: Optional<ActorDefinitionVersionWithOverrideStatus> =
      overrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID)

    Assertions.assertTrue(optResult.isPresent())
    Assertions.assertEquals(OVERRIDE_VERSION, optResult.get().actorDefinitionVersion)
  }

  companion object {
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
    private val ACTOR_ID: UUID = UUID.randomUUID()
    private const val DOCKER_REPOSITORY = "airbyte/source-test"
    private const val DOCKER_IMAGE_TAG = "0.1.0"
    private const val DOCKER_IMAGE_TAG_2 = "2.0.2"
    private const val DOCS_URL = "https://airbyte.io/docs/"
    private val ALLOWED_HOSTS: AllowedHosts = AllowedHosts().withHosts(listOf("https://airbyte.io"))
    private val SUGGESTED_STREAMS: SuggestedStreams = SuggestedStreams().withStreams(listOf("users"))
    private val SPEC: ConnectorSpecification =
      ConnectorSpecification()
        .withProtocolVersion("0.2.0")
        .withConnectionSpecification(
          jsonNode(
            Map.of(
              "key",
              "value",
            ),
          ),
        )
    private val SPEC_2: ConnectorSpecification =
      ConnectorSpecification()
        .withProtocolVersion("0.2.0")
        .withConnectionSpecification(
          jsonNode(
            Map.of(
              "theSpec",
              "goesHere",
            ),
          ),
        )
    private val DEFAULT_VERSION: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withDockerRepository(DOCKER_REPOSITORY)
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC)
        .withProtocolVersion(SPEC.getProtocolVersion())
        .withDocumentationUrl(DOCS_URL)
        .withReleaseStage(ReleaseStage.BETA)
        .withSuggestedStreams(SUGGESTED_STREAMS)
        .withAllowedHosts(ALLOWED_HOSTS)
    private val OVERRIDE_VERSION: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withDockerRepository(DOCKER_REPOSITORY)
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerImageTag(DOCKER_IMAGE_TAG_2)
        .withSpec(SPEC_2)
        .withProtocolVersion(SPEC_2.getProtocolVersion())
        .withDocumentationUrl(DOCS_URL)
        .withReleaseStage(ReleaseStage.BETA)
        .withSuggestedStreams(SUGGESTED_STREAMS)
        .withAllowedHosts(ALLOWED_HOSTS)
  }
}
