/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.ScopedConfigurationMetadata
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class ScopedConfigurationAuthorizationServiceTest {
  private val data = mockk<AuthorizationDataService>()
  private val configs = mockk<ScopedConfigurationService>()
  private val definitions = mockk<ActorDefinitionService>()
  private val service = ScopedConfigurationAuthorizationService(data, configs, definitions)
  private val id = UUID(0, 1)
  private val workspace = UUID(0, 2)
  private val organization = UUID(0, 3)
  private val definition = UUID(0, 4)
  private val version = UUID(0, 5)
  private val user = UUID(0, 6)

  private fun config() =
    ScopedConfiguration()
      .withId(id)
      .withKey("connector_version")
      .withValue(version.toString())
      .withScopeType(
        ConfigScopeType.WORKSPACE,
      ).withScopeId(
        workspace,
      ).withResourceType(
        ConfigResourceType.ACTOR_DEFINITION,
      ).withResourceId(definition)
      .withOriginType(ConfigOriginType.USER)
      .withOrigin(user.toString())

  @BeforeEach
  fun parents() {
    every { data.savedWorkspaceAndOrganization(workspace, true) } returns WorkspaceAndOrganization(workspace, organization)
    every { data.requireSavedActorDefinition(definition) } just Runs
    every { data.requireUser(user) } just Runs
  }

  private fun saved(config: ScopedConfiguration) {
    every { configs.getScopedConfigurationMetadata(id) } answers
      {
        ScopedConfigurationMetadata(
          config.id,
          config.key,
          config.scopeType,
          config.scopeId,
          Optional.ofNullable(config.resourceType),
          Optional.ofNullable(config.resourceId),
          config.originType,
          config.origin,
        )
      }
    every { configs.getConnectorVersionSelector(id) } answers { config.value }
  }

  @Test
  fun `authorization of arbitrary config values never fetches the full secret bearing row`() {
    saved(config().withKey("network_security_token").withValue("secret-token-that-must-not-be-read"))
    every { configs.getScopedConfiguration(id) } throws AssertionError("secret-bearing full row must not be fetched")
    assertEquals(workspace, service.existing(id).orElseThrow().scopeId)
    assertEquals(workspace, service.read(id).scopeId)
    assertEquals(workspace, service.deletion(id).scopeId)
    verify(exactly = 0) { configs.getScopedConfiguration(any()) }
    verify(exactly = 0) { configs.getConnectorVersionSelector(any()) }
  }

  @Test
  fun `nondefinition resource fields are display labels rather than definition selectors`() {
    val displayId = UUID(0, 99)
    saved(config().withKey("network_security_token").withResourceType(ConfigResourceType.USER).withResourceId(displayId))
    every { data.savedWorkspaceAndOrganization(workspace, true) } returns WorkspaceAndOrganization(workspace, organization)
    every { data.requireUser(user) } just Runs
    every { data.requireSavedActorDefinition(displayId) } throws AuthorizationResourceNotFoundException()
    assertDoesNotThrow { service.validate(service.read(id)) }
    verify(exactly = 0) { data.requireSavedActorDefinition(displayId) }
  }

  @Test
  fun `connector version selector is never read until saved scope and definition parents are valid`() {
    saved(config())
    every { data.savedWorkspaceAndOrganization(workspace, true) } throws AuthorizationResourceNotFoundException()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.read(id) }
    verify(exactly = 0) { configs.getConnectorVersionSelector(any()) }
  }

  @Test
  fun `saved references use exact config ID and immutable semantic scope definition user and version fields`() {
    val config = config()
    saved(config)
    val references = service.read(id)
    config.scopeId = UUID(0, 99)
    assertEquals(workspace, references.scopeId)
    assertEquals(Optional.of(definition), references.definition)
    assertEquals(Optional.of(user), references.user)
    assertEquals(Optional.of(version), references.version)
    config.id = UUID(0, 99)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.read(id) }
  }

  @Test
  fun `upsert absence is explicit and never absorbs ownership dependency failures`() {
    every { configs.getScopedConfigurationMetadata(id) } throws ConfigNotFoundException(ConfigNotFoundType.SCOPED_CONFIGURATION, id)
    assertFalse(service.existing(id).isPresent)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.read(id) }
    val failure = IOException("scoped config unavailable")
    every { configs.getScopedConfigurationMetadata(id) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.existing(id) })
  }

  @Test
  fun `semantic version belongs to selected definition while opaque values and nonuser origins remain opaque`() {
    saved(config())
    every { data.savedWorkspaceAndOrganization(workspace, true) } returns WorkspaceAndOrganization(workspace, organization)
    every { data.requireSavedActorDefinition(definition) } just Runs
    every { data.requireUser(user) } just Runs
    every { data.savedDefinitionVersionParent(version) } returns definition
    service.validate(service.read(id))
    every { data.savedDefinitionVersionParent(version) } returns UUID(0, 99)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.validate(service.read(id)) }
    saved(
      config()
        .withKey(
          "network_security_token",
        ).withValue("opaque-token")
        .withOriginType(ConfigOriginType.PRIVATE_LINK)
        .withOrigin("external-tracking"),
    )
    assertFalse(service.read(id).version.isPresent)
    assertFalse(service.read(id).user.isPresent)
  }

  @Test
  fun `deleted actor names are supported for config enrichment while context requires actual saved ancestors`() {
    val actor = UUID(0, 7)
    saved(config().withScopeType(ConfigScopeType.ACTOR).withScopeId(actor))
    every { data.existingSavedActorWorkspaceAndOrganization(actor) } returns Optional.empty()
    every { data.requireSavedActorDefinition(definition) } just Runs
    every { data.requireUser(user) } just Runs
    every { data.savedDefinitionVersionParent(version) } returns definition
    service.validate(service.read(id))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireActorContext(actor) }
    val failure = IOException("actor metadata unavailable")
    every { data.existingSavedActorWorkspaceAndOrganization(actor) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.validate(service.read(id)) })
  }

  @Test
  fun `ordinary deletion and unpin without default do not consume ignored origin or value metadata`() {
    saved(config().withKey("network_security_token").withValue("opaque").withOrigin("not-a-user-id"))
    val ordinary = service.deletion(id)
    assertFalse(ordinary.user.isPresent)
    assertFalse(ordinary.version.isPresent)
    assertFalse(ordinary.definition.isPresent)
    saved(config().withValue("stale-non-uuid").withOrigin("not-a-user-id"))
    every { definitions.getDefaultVersionForActorDefinitionIdOptional(definition) } returns Optional.empty()
    val unpin = service.deletion(id)
    assertFalse(unpin.version.isPresent)
    verify(exactly = 0) { data.savedDefinitionVersionParent(any()) }
  }
}
