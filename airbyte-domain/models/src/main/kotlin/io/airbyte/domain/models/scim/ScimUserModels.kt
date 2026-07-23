/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.scim

import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.OffsetDateTime
import java.util.UUID

data class ScimUserWrite(
  val userName: String,
  val externalId: String?,
  val primaryEmail: String,
  val active: Boolean,
  val attributes: ObjectNode,
)

data class ScimUserRead(
  val id: UUID,
  val configurationId: UUID,
  val organizationId: UUID,
  val userId: UUID,
  val externalId: String?,
  val userName: String,
  val primaryEmail: String,
  val active: Boolean,
  val attributes: ObjectNode,
  val createdAt: OffsetDateTime,
  val updatedAt: OffsetDateTime,
  val groups: List<ScimUserGroup> = emptyList(),
)

data class ScimUserListPage(
  val resources: List<ScimUserRead>,
  val totalResults: Int,
)

data class ScimUserGroup(
  val id: UUID,
  val displayName: String,
)

enum class ScimUserFilterAttribute {
  USER_NAME,
  EXTERNAL_ID,
  EMAIL,
  WORK_EMAIL,
}

data class ScimUserFilterClause(
  val attribute: ScimUserFilterAttribute,
  val value: String,
)

class ScimUserNotFoundException : RuntimeException("SCIM User was not found")

class ScimUserConflictException : RuntimeException("The SCIM User identifier is already in use")
