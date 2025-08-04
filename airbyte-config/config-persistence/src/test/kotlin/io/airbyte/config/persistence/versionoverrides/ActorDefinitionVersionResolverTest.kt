/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.versionoverrides

import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.AllowedHosts
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.ReleaseStage
import io.airbyte.config.SuggestedStreams
import io.airbyte.config.helpers.ConnectorRegistryConverters.toActorDefinitionVersion
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.util.Map
import java.util.Optional
import java.util.UUID

internal class ActorDefinitionVersionResolverTest {
  private lateinit var actorDefinitionVersionResolver: ActorDefinitionVersionResolver
  private lateinit var remoteDefinitionsProvider: RemoteDefinitionsProvider
  private lateinit var actorDefinitionService: ActorDefinitionService

  @BeforeEach
  fun setup() {
    remoteDefinitionsProvider = Mockito.mock(RemoteDefinitionsProvider::class.java)
    actorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    actorDefinitionVersionResolver = ActorDefinitionVersionResolver(remoteDefinitionsProvider, actorDefinitionService)
  }

  @Test
  @Throws(IOException::class)
  fun testResolveVersionFromDB() {
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG))
      .thenReturn(
        Optional.of(ACTOR_DEFINITION_VERSION),
      )

    Assertions.assertEquals(
      Optional.of(ACTOR_DEFINITION_VERSION),
      actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG),
    )

    Mockito.verify(actorDefinitionService).getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    Mockito.verifyNoMoreInteractions(actorDefinitionService)

    Mockito.verifyNoInteractions(remoteDefinitionsProvider)
  }

  @Test
  @Throws(IOException::class)
  fun testResolveVersionFromRemoteIfNotInDB() {
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG))
      .thenReturn(
        Optional.empty(),
      )
    Mockito
      .`when`(
        remoteDefinitionsProvider.getSourceDefinitionByVersion(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
        ),
      ).thenReturn(
        Optional.of(REGISTRY_DEF),
      )

    val actorDefinitionVersion: ActorDefinitionVersion =
      toActorDefinitionVersion(REGISTRY_DEF)
    val persistedAdv =
      clone(actorDefinitionVersion).withVersionId(UUID.randomUUID())
    Mockito.`when`(actorDefinitionService.writeActorDefinitionVersion(actorDefinitionVersion)).thenReturn(persistedAdv)

    val optResult: Optional<ActorDefinitionVersion> =
      actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optResult.isPresent())
    Assertions.assertEquals(persistedAdv, optResult.get())

    Mockito.verify(actorDefinitionService).getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    Mockito.verify(actorDefinitionService).writeActorDefinitionVersion(actorDefinitionVersion)
    Mockito.verifyNoMoreInteractions(actorDefinitionService)

    Mockito.verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)
    Mockito.verifyNoMoreInteractions(remoteDefinitionsProvider)
  }

  @Test
  @Throws(IOException::class)
  fun testReturnsEmptyOptionalIfNoVersionFoundInDbOrRemote() {
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG))
      .thenReturn(
        Optional.empty(),
      )
    Mockito
      .`when`(
        remoteDefinitionsProvider.getSourceDefinitionByVersion(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
        ),
      ).thenReturn(
        Optional.empty(),
      )

    Assertions.assertTrue(
      actorDefinitionVersionResolver
        .resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)
        .isEmpty(),
    )

    Mockito.verify(actorDefinitionService).getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    Mockito.verifyNoMoreInteractions(actorDefinitionService)

    Mockito.verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)
    Mockito.verifyNoMoreInteractions(remoteDefinitionsProvider)
  }

  companion object {
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private const val DOCKER_REPOSITORY = "airbyte/source-test"
    private const val DOCKER_IMAGE_TAG = "0.1.0"
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
    private const val DOCS_URL = "https://airbyte.io/docs/"
    private val ALLOWED_HOSTS: AllowedHosts = AllowedHosts().withHosts(listOf("https://airbyte.io"))
    private val SUGGESTED_STREAMS: SuggestedStreams = SuggestedStreams().withStreams(listOf("users"))
    private val ACTOR_DEFINITION_VERSION: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DOCKER_REPOSITORY)
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC)
        .withProtocolVersion(SPEC.getProtocolVersion())
        .withDocumentationUrl(DOCS_URL)
        .withReleaseStage(ReleaseStage.BETA)
        .withSuggestedStreams(SUGGESTED_STREAMS)
        .withAllowedHosts(ALLOWED_HOSTS)

    private val REGISTRY_DEF: ConnectorRegistrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(ACTOR_DEFINITION_VERSION.getDockerRepository())
        .withDockerImageTag(ACTOR_DEFINITION_VERSION.getDockerImageTag())
        .withSpec(ACTOR_DEFINITION_VERSION.getSpec())
        .withProtocolVersion(ACTOR_DEFINITION_VERSION.getProtocolVersion())
        .withDocumentationUrl(ACTOR_DEFINITION_VERSION.getDocumentationUrl())
        .withReleaseStage(ACTOR_DEFINITION_VERSION.getReleaseStage())
        .withSuggestedStreams(ACTOR_DEFINITION_VERSION.getSuggestedStreams())
        .withAllowedHosts(ACTOR_DEFINITION_VERSION.getAllowedHosts())
  }
}
