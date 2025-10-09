/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
}
