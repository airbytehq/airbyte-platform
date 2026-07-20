/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID

@MicronautTest
class ScimConfigurationRepositoryTest : AbstractConfigRepositoryTest() {
  private val scimConfigurationRepository = context.getBean(ScimConfigurationRepository::class.java)

  @AfterEach
  fun cleanUp() {
    scimConfigurationRepository.deleteAll()
    organizationRepository.deleteAll()
  }

  @Test
  fun `persists and retrieves an enabled configuration by organization`() {
    val organization = createOrganization("enabled")
    val tokenIssuedAt = OffsetDateTime.now().truncatedTo(ChronoUnit.MICROS)
    val configuration =
      ScimConfiguration(
        organizationId = organization.id!!,
        tokenHash = "hash",
        idpProvider = "okta",
        enabled = true,
        tokenIssuedAt = tokenIssuedAt,
      )

    val saved = scimConfigurationRepository.save(configuration)

    assertThat(scimConfigurationRepository.findByOrganizationId(organization.id!!))
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(saved)
    assertThat(saved.createdAt).isNotNull()
    assertThat(saved.updatedAt).isNotNull()
  }

  @Test
  fun `persists and retrieves a disabled configuration`() {
    val organization = createOrganization("disabled")
    val disabledAt = OffsetDateTime.now().truncatedTo(ChronoUnit.MICROS)
    val configuration =
      ScimConfiguration(
        organizationId = organization.id!!,
        idpProvider = "microsoft_entra_id",
        enabled = false,
        disabledAt = disabledAt,
      )

    scimConfigurationRepository.save(configuration)

    val retrieved = scimConfigurationRepository.findByOrganizationIdForUpdate(organization.id!!)
    assertThat(retrieved?.enabled).isFalse()
    assertThat(retrieved?.tokenHash).isNull()
    assertThat(retrieved?.tokenIssuedAt).isNull()
    assertThat(retrieved?.idpProvider).isEqualTo("microsoft_entra_id")
    assertThat(retrieved?.disabledAt).isEqualTo(disabledAt)
  }

  @Test
  fun `organization scoped lookups do not return another organization's configuration`() {
    val configuredOrganization = createOrganization("configured")
    val otherOrganization = createOrganization("other")
    scimConfigurationRepository.save(
      ScimConfiguration(
        organizationId = configuredOrganization.id!!,
        tokenHash = "configured-hash",
        idpProvider = "okta",
        enabled = true,
        tokenIssuedAt = OffsetDateTime.now(),
      ),
    )

    assertNull(scimConfigurationRepository.findByOrganizationId(otherOrganization.id!!))
    assertNull(scimConfigurationRepository.findByOrganizationIdForUpdate(otherOrganization.id!!))
  }

  @Test
  fun `lifecycle updates require the configuration and organization ids to match`() {
    val configuredOrganization = createOrganization("configured-update")
    val otherOrganization = createOrganization("other-update")
    val issuedAt = OffsetDateTime.now().truncatedTo(ChronoUnit.MICROS)
    val saved =
      scimConfigurationRepository.save(
        ScimConfiguration(
          organizationId = configuredOrganization.id!!,
          tokenHash = "original-hash",
          idpProvider = "okta",
          enabled = true,
          tokenIssuedAt = issuedAt,
        ),
      )
    val attemptedAt = issuedAt.plusMinutes(1)

    val rotated =
      scimConfigurationRepository.rotateTokenByIdAndOrganizationId(
        id = saved.id!!,
        organizationId = otherOrganization.id!!,
        tokenHash = "replacement-hash",
        tokenIssuedAt = attemptedAt,
        tokenIssuedByUserId = UUID.randomUUID(),
        updatedAt = attemptedAt,
      )
    val disabled =
      scimConfigurationRepository.disableByIdAndOrganizationId(
        id = saved.id!!,
        organizationId = otherOrganization.id!!,
        disabledAt = attemptedAt,
        disabledByUserId = UUID.randomUUID(),
        updatedAt = attemptedAt,
      )

    assertThat(rotated).isZero()
    assertThat(disabled).isZero()
    val unchanged = scimConfigurationRepository.findByOrganizationId(configuredOrganization.id!!)
    assertThat(unchanged?.organizationId).isEqualTo(configuredOrganization.id!!)
    assertThat(unchanged?.tokenHash).isEqualTo("original-hash")
    assertThat(unchanged?.enabled).isTrue()
    assertThat(unchanged?.tokenIssuedAt).isEqualTo(issuedAt)
    assertThat(unchanged?.disabledAt).isNull()
  }

  private fun createOrganization(name: String): Organization =
    organizationRepository.save(
      Organization(
        name = name,
        email = "$name@example.com",
      ),
    )
}
