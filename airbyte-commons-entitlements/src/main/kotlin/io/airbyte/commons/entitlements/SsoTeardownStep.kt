/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.BeanProvider
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Removes an organization's SSO setup once its plan no longer includes SSO: the Keycloak realm, the SSO config,
 * the email domains that enforce SSO login, and any SCIM configuration (SCIM provisions into the realm).
 *
 * Keycloak is deleted first so a Keycloak failure leaves the database untouched and the step can be retried.
 * If the realm is already gone because an earlier run failed after deleting it, the database cleanup still runs.
 * Users, org membership and permissions are left alone; SSO-only users sign in again through the Cloud realm.
 */
@Singleton
internal class SsoTeardownStep(
  private val ssoConfigService: SsoConfigService,
  private val organizationEmailDomainService: OrganizationEmailDomainService,
  private val scimConfigurationRepository: ScimConfigurationRepository,
  private val airbyteKeycloakClient: BeanProvider<AirbyteKeycloakClient>,
) {
  fun run(organizationId: OrganizationId): PlanLimitStepResult {
    val ssoConfig = ssoConfigService.getSsoConfig(organizationId.value) ?: return PlanLimitStepResult.NotNeeded
    val realm = ssoConfig.keycloakRealm

    val keycloakClient = airbyteKeycloakClient.get()
    if (keycloakClient.realmExists(realm)) {
      logger.info { "Deleting Keycloak realm for organization that is no longer entitled to SSO. organizationId=$organizationId realm=$realm" }
      keycloakClient.deleteRealm(realm)
    } else {
      logger.info { "Keycloak realm already absent, finishing SSO cleanup. organizationId=$organizationId realm=$realm" }
    }

    ssoConfigService.deleteSsoConfig(organizationId.value)
    organizationEmailDomainService.deleteAllEmailDomains(organizationId.value)
    scimConfigurationRepository.findByOrganizationId(organizationId.value)?.let { scimConfigurationRepository.delete(it) }

    return PlanLimitStepResult.Completed("removed SSO realm=$realm")
  }
}
