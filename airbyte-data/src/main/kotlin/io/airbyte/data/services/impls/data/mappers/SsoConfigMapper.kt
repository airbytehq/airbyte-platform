/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.SsoConfig
import io.airbyte.domain.models.DEFAULT_SSO_ROLE

typealias EntitySsoConfig = SsoConfig
typealias EntitySsoConfigStatus = io.airbyte.db.instance.configs.jooq.generated.enums.SsoConfigStatus
typealias ConfigSsoConfig = io.airbyte.config.SsoConfig
typealias DomainSsoConfigStatus = io.airbyte.domain.models.SsoConfigStatus
typealias ConfigSsoConfigStatus = io.airbyte.config.SsoConfigStatus
typealias ApiSsoConfigStatus = io.airbyte.api.server.generated.models.SSOConfigStatus
typealias ApiSsoConfigDefaultRole = io.airbyte.api.server.generated.models.SSOConfigDefaultRole

fun DomainSsoConfigStatus.toEntity(): EntitySsoConfigStatus =
  when (this) {
    DomainSsoConfigStatus.DRAFT -> EntitySsoConfigStatus.draft
    DomainSsoConfigStatus.ACTIVE -> EntitySsoConfigStatus.active
  }

fun DomainSsoConfigStatus.toApi(): ApiSsoConfigStatus =
  when (this) {
    DomainSsoConfigStatus.DRAFT -> ApiSsoConfigStatus.DRAFT
    DomainSsoConfigStatus.ACTIVE -> ApiSsoConfigStatus.ACTIVE
  }

fun ApiSsoConfigStatus.toDomain(): DomainSsoConfigStatus =
  when (this) {
    ApiSsoConfigStatus.DRAFT -> DomainSsoConfigStatus.DRAFT
    ApiSsoConfigStatus.ACTIVE -> DomainSsoConfigStatus.ACTIVE
  }

fun DomainSsoDefaultRole.toApi(): ApiSsoConfigDefaultRole =
  when (this) {
    DomainSsoDefaultRole.ORGANIZATION_ADMIN -> ApiSsoConfigDefaultRole.ADMIN
    DomainSsoDefaultRole.ORGANIZATION_EDITOR -> ApiSsoConfigDefaultRole.EDITOR
    DomainSsoDefaultRole.ORGANIZATION_MEMBER -> ApiSsoConfigDefaultRole.MEMBER
  }

fun ApiSsoConfigDefaultRole.toDomain(): DomainSsoDefaultRole =
  when (this) {
    ApiSsoConfigDefaultRole.ADMIN -> DomainSsoDefaultRole.ORGANIZATION_ADMIN
    ApiSsoConfigDefaultRole.EDITOR -> DomainSsoDefaultRole.ORGANIZATION_EDITOR
    ApiSsoConfigDefaultRole.MEMBER -> DomainSsoDefaultRole.ORGANIZATION_MEMBER
  }

fun EntitySsoConfig.toConfigModel(): ConfigSsoConfig =
  ConfigSsoConfig()
    .withSsoConfigId(this.id)
    .withOrganizationId(this.organizationId)
    .withKeycloakRealm(this.keycloakRealm)
    .withStatus(this.status.toConfig())
    .withDefaultRole((this.defaultRole?.toConfigModel() ?: DEFAULT_SSO_ROLE.toConfigModel()))

fun ConfigSsoConfigStatus.toDomain(): DomainSsoConfigStatus =
  when (this) {
    ConfigSsoConfigStatus.DRAFT -> DomainSsoConfigStatus.DRAFT
    ConfigSsoConfigStatus.ACTIVE -> DomainSsoConfigStatus.ACTIVE
  }

fun EntitySsoConfigStatus.toConfig(): ConfigSsoConfigStatus =
  when (this) {
    EntitySsoConfigStatus.draft -> ConfigSsoConfigStatus.DRAFT
    EntitySsoConfigStatus.active -> ConfigSsoConfigStatus.ACTIVE
  }
