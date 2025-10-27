/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

/**
 * DTO for queries that join organization with SSO config to include the SSO realm.
 * This avoids the N+1 query problem and PostgreSQL parameter limit issues.
 */
@MappedEntity
data class OrganizationWithSsoRealm(
  val id: UUID,
  val name: String,
  val userId: UUID?,
  val email: String,
  val tombstone: Boolean,
  val createdAt: java.time.OffsetDateTime?,
  val updatedAt: java.time.OffsetDateTime?,
  val keycloakRealm: String?,
)
