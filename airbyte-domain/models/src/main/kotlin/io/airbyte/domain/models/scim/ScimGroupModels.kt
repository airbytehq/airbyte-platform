/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.scim

import java.time.OffsetDateTime
import java.util.UUID

data class ScimGroupWrite(
  val displayName: String,
  val externalId: String?,
  val memberIds: List<UUID>,
)

data class ScimGroupRead(
  val id: UUID,
  val configurationId: UUID,
  val organizationId: UUID,
  val groupId: UUID,
  val externalId: String?,
  val displayName: String,
  val createdAt: OffsetDateTime,
  val updatedAt: OffsetDateTime,
  val members: List<ScimGroupMember>,
)

data class ScimGroupListPage(
  val resources: List<ScimGroupRead>,
  val totalResults: Int,
)

data class ScimGroupMember(
  val id: UUID,
  val userId: UUID,
  val display: String,
)

enum class ScimGroupFilterAttribute {
  DISPLAY_NAME,
  MEMBER,
}

data class ScimGroupFilterClause(
  val attribute: ScimGroupFilterAttribute,
  val value: String,
)

class ScimGroupNotFoundException : RuntimeException("SCIM Group was not found")

class ScimGroupConflictException : RuntimeException("The SCIM Group identifier or name is already in use")

class ScimGroupInvalidMemberException : RuntimeException("Group members must be active Users in the authenticated SCIM tenant")
