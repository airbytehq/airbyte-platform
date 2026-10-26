/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.mockk.Called
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

internal class ActorDefinitionVersionResolverTest {
  private lateinit var actorDefinitionVersionResolver: ActorDefinitionVersionResolver
  private lateinit var remoteDefinitionsProvider: RemoteDefinitionsProvider
  private lateinit var actorDefinitionService: ActorDefinitionService

  @BeforeEach
  fun setup() {
    remoteDefinitionsProvider = mockk<RemoteDefinitionsProvider>()
    actorDefinitionService = mockk<ActorDefinitionService>()
    actorDefinitionVersionResolver = ActorDefinitionVersionResolver(remoteDefinitionsProvider, actorDefinitionService)
  }

  @Test
  fun testResolveVersionFromDB() {
    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns Optional.of(ACTOR_DEFINITION_VERSION)

    Assertions.assertEquals(
      Optional.of(ACTOR_DEFINITION_VERSION),
      actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG),
    )

    verify { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) }
    confirmVerified(actorDefinitionService)

    verify { remoteDefinitionsProvider wasNot Called }
  }

  @Test
  fun testResolveVersionFromRemoteIfNotInDB() {
    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns Optional.empty()
    every {
      remoteDefinitionsProvider.getSourceDefinitionByVersion(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
      )
    } returns Optional.of(REGISTRY_DEF)

    val actorDefinitionVersion: ActorDefinitionVersion =
      toActorDefinitionVersion(REGISTRY_DEF)
    val persistedAdv =
      clone(actorDefinitionVersion).withVersionId(UUID.randomUUID())
    every { actorDefinitionService.writeActorDefinitionVersion(actorDefinitionVersion) } returns persistedAdv

    val optResult: Optional<ActorDefinitionVersion> =
      actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optResult.isPresent)
    Assertions.assertEquals(persistedAdv, optResult.get())

    verify { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) }
    verify { actorDefinitionService.writeActorDefinitionVersion(actorDefinitionVersion) }
    confirmVerified(actorDefinitionService)

    verify { remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG) }
    confirmVerified(remoteDefinitionsProvider)
  }

  @Test
  fun testReturnsEmptyOptionalIfNoVersionFoundInDbOrRemote() {
    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns Optional.empty()
    every {
      remoteDefinitionsProvider.getSourceDefinitionByVersion(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
      )
    } returns Optional.empty()

    Assertions.assertTrue(
      actorDefinitionVersionResolver
        .resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)
        .isEmpty,
    )

    verify { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) }
    confirmVerified(actorDefinitionService)

    verify { remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG) }
    confirmVerified(remoteDefinitionsProvider)
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
            mapOf(
              "key" to "value",
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
        .withProtocolVersion(SPEC.protocolVersion)
        .withDocumentationUrl(DOCS_URL)
        .withReleaseStage(ReleaseStage.BETA)
        .withSuggestedStreams(SUGGESTED_STREAMS)
        .withAllowedHosts(ALLOWED_HOSTS)

    private val REGISTRY_DEF: ConnectorRegistrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(ACTOR_DEFINITION_VERSION.dockerRepository)
        .withDockerImageTag(ACTOR_DEFINITION_VERSION.dockerImageTag)
        .withSpec(ACTOR_DEFINITION_VERSION.spec)
        .withProtocolVersion(ACTOR_DEFINITION_VERSION.protocolVersion)
        .withDocumentationUrl(ACTOR_DEFINITION_VERSION.documentationUrl)
        .withReleaseStage(ACTOR_DEFINITION_VERSION.releaseStage)
        .withSuggestedStreams(ACTOR_DEFINITION_VERSION.suggestedStreams)
        .withAllowedHosts(ACTOR_DEFINITION_VERSION.allowedHosts)
  }
}
