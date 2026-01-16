/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.SsoConfig
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.SsoConfigStatus
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class SsoConfigRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      jooqDslContext
        .alterTable(
          Tables.SSO_CONFIG,
        ).dropForeignKey(Keys.SSO_CONFIG__SSO_CONFIG_ORGANIZATION_ID_FKEY.constraint())
        .execute()
    }
  }

  @AfterEach
  fun tearDown() {
    organizationEmailDomainRepository.deleteAll()
  }

  @Test
  fun `save should return a sso config`() {
    val ssoConfig =
      SsoConfig(
        id = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        keycloakRealm = "realm-name",
        status = SsoConfigStatus.active,
      )

    val saved = ssoConfigRepository.save(ssoConfig)

    val retrieved = ssoConfigRepository.findById(saved.id)
    assertTrue(retrieved.isPresent)
    assertThat(retrieved.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(ssoConfig)
  }

  @Test
  fun `findByKeycloakRealm should return sso config when realm exists`() {
    val ssoConfig =
      SsoConfig(
        id = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        keycloakRealm = "test-realm",
        status = SsoConfigStatus.active,
      )

    ssoConfigRepository.save(ssoConfig)

    val retrieved = ssoConfigRepository.findByKeycloakRealm("test-realm")
    assertThat(retrieved).isNotNull
    assertThat(retrieved!!.keycloakRealm).isEqualTo("test-realm")
    assertThat(retrieved.organizationId).isEqualTo(ssoConfig.organizationId)
  }

  @Test
  fun `findByKeycloakRealm should return null when realm does not exist`() {
    val retrieved = ssoConfigRepository.findByKeycloakRealm("non-existent-realm")
    assertThat(retrieved).isNull()
  }

  @Test
  fun `findByOrganizationIdIn should return sso configs for multiple organizations`() {
    val org1Id = UUID.randomUUID()
    val org2Id = UUID.randomUUID()
    val org3Id = UUID.randomUUID()

    val ssoConfig1 =
      SsoConfig(
        id = UUID.randomUUID(),
        organizationId = org1Id,
        keycloakRealm = "realm-1",
        status = SsoConfigStatus.active,
      )
    val ssoConfig2 =
      SsoConfig(
        id = UUID.randomUUID(),
        organizationId = org2Id,
        keycloakRealm = "realm-2",
        status = SsoConfigStatus.active,
      )
    val ssoConfig3 =
      SsoConfig(
        id = UUID.randomUUID(),
        organizationId = org3Id,
        keycloakRealm = "realm-3",
        status = SsoConfigStatus.draft,
      )

    ssoConfigRepository.save(ssoConfig1)
    ssoConfigRepository.save(ssoConfig2)
    ssoConfigRepository.save(ssoConfig3)

    // Query for org1 and org3
    val retrieved = ssoConfigRepository.findByOrganizationIdIn(listOf(org1Id, org3Id))

    assertThat(retrieved).hasSize(2)
    assertThat(retrieved.map { it.organizationId }).containsExactlyInAnyOrder(org1Id, org3Id)
    assertThat(retrieved.map { it.keycloakRealm }).containsExactlyInAnyOrder("realm-1", "realm-3")
  }

  @Test
  fun `findByOrganizationIdIn should return empty list when no matches found`() {
    val org1Id = UUID.randomUUID()
    val org2Id = UUID.randomUUID()

    // Don't create any SSO configs
    val retrieved = ssoConfigRepository.findByOrganizationIdIn(listOf(org1Id, org2Id))

    assertThat(retrieved).isEmpty()
  }

  @Test
  fun `findByOrganizationIdIn should return empty list for empty input`() {
    val retrieved = ssoConfigRepository.findByOrganizationIdIn(emptyList())

    assertThat(retrieved).isEmpty()
  }

  @Test
  fun `findByOrganizationIdIn should handle single organization id`() {
    val orgId = UUID.randomUUID()

    val ssoConfig =
      SsoConfig(
        id = UUID.randomUUID(),
        organizationId = orgId,
        keycloakRealm = "single-realm",
        status = SsoConfigStatus.active,
      )

    ssoConfigRepository.save(ssoConfig)

    val retrieved = ssoConfigRepository.findByOrganizationIdIn(listOf(orgId))

    assertThat(retrieved).hasSize(1)
    assertThat(retrieved[0].organizationId).isEqualTo(orgId)
    assertThat(retrieved[0].keycloakRealm).isEqualTo("single-realm")
  }
}
