/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.SsoConfig

typealias EntitySsoConfig = SsoConfig
typealias ConfigSsoConfig = io.airbyte.config.SsoConfig

fun EntitySsoConfig.toConfigModel(): ConfigSsoConfig =
  ConfigSsoConfig()
    .withSsoConfigId(this.id)
    .withOrganizationId(this.organizationId)
    .withKeycloakRealm(this.keycloakRealm)
