/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.micronaut.core.annotation.Introspected
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ScimResourceMappingRepository : PageableRepository<ScimResourceMapping, UUID> {
  @Query(
    """
    SELECT COUNT(*)
    FROM scim_resource_mapping group_mapping
    JOIN "group" managed_group
      ON managed_group.id = group_mapping.group_id
     AND managed_group.organization_id = :organizationId
    WHERE group_mapping.scim_configuration_id = :configurationId
      AND group_mapping.organization_id = :organizationId
      AND group_mapping.resource_type = 'GROUP'
      AND (CAST(:displayName AS text) IS NULL OR lower(managed_group.name) = lower(CAST(:displayName AS text)))
      AND (
        CAST(:memberId AS text) IS NULL OR EXISTS (
          SELECT 1
          FROM group_member membership
          JOIN scim_resource_mapping user_mapping
            ON user_mapping.user_id = membership.user_id
           AND user_mapping.scim_configuration_id = :configurationId
           AND user_mapping.organization_id = :organizationId
           AND user_mapping.resource_type = 'USER'
           AND user_mapping.user_active = TRUE
          WHERE membership.group_id = managed_group.id
            AND CAST(user_mapping.id AS text) = CAST(:memberId AS text)
        )
      )
    """,
  )
  fun countGroups(
    configurationId: UUID,
    organizationId: UUID,
    displayName: String?,
    memberId: String?,
  ): Long

  @Query(
    """
    SELECT group_mapping.id,
           group_mapping.scim_configuration_id,
           group_mapping.organization_id,
           group_mapping.group_id,
           group_mapping.external_id,
           managed_group.name AS display_name,
           group_mapping.created_at,
           group_mapping.updated_at
    FROM scim_resource_mapping group_mapping
    JOIN "group" managed_group
      ON managed_group.id = group_mapping.group_id
     AND managed_group.organization_id = :organizationId
    WHERE group_mapping.scim_configuration_id = :configurationId
      AND group_mapping.organization_id = :organizationId
      AND group_mapping.resource_type = 'GROUP'
      AND (CAST(:displayName AS text) IS NULL OR lower(managed_group.name) = lower(CAST(:displayName AS text)))
      AND (
        CAST(:memberId AS text) IS NULL OR EXISTS (
          SELECT 1
          FROM group_member membership
          JOIN scim_resource_mapping user_mapping
            ON user_mapping.user_id = membership.user_id
           AND user_mapping.scim_configuration_id = :configurationId
           AND user_mapping.organization_id = :organizationId
           AND user_mapping.resource_type = 'USER'
           AND user_mapping.user_active = TRUE
          WHERE membership.group_id = managed_group.id
            AND CAST(user_mapping.id AS text) = CAST(:memberId AS text)
        )
      )
    ORDER BY group_mapping.created_at, group_mapping.id
    LIMIT :limit OFFSET :offset
    """,
  )
  fun findGroupsPage(
    configurationId: UUID,
    organizationId: UUID,
    displayName: String?,
    memberId: String?,
    offset: Long,
    limit: Int,
  ): List<ScimGroupRow>

  @Query(
    """
    SELECT group_mapping.id,
           group_mapping.scim_configuration_id,
           group_mapping.organization_id,
           group_mapping.group_id,
           group_mapping.external_id,
           managed_group.name AS display_name,
           group_mapping.created_at,
           group_mapping.updated_at
    FROM scim_resource_mapping group_mapping
    JOIN "group" managed_group
      ON managed_group.id = group_mapping.group_id
     AND managed_group.organization_id = :organizationId
    WHERE group_mapping.id = :id
      AND group_mapping.scim_configuration_id = :configurationId
      AND group_mapping.organization_id = :organizationId
      AND group_mapping.resource_type = 'GROUP'
    """,
  )
  fun findGroup(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): ScimGroupRow?

  @Query(
    """
    SELECT * FROM scim_resource_mapping
    WHERE id = :id
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'GROUP'
    FOR UPDATE
    """,
  )
  fun findGroupForUpdate(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): ScimResourceMapping?

  @Query(
    """
    SELECT group_mapping.scim_configuration_id,
           configuration.enabled
    FROM scim_resource_mapping group_mapping
    JOIN scim_configuration configuration
      ON configuration.id = group_mapping.scim_configuration_id
     AND configuration.organization_id = :organizationId
    JOIN "group" managed_group
      ON managed_group.id = group_mapping.group_id
     AND managed_group.organization_id = :organizationId
    WHERE group_mapping.group_id = :groupId
      AND group_mapping.organization_id = :organizationId
      AND group_mapping.resource_type = 'GROUP'
    """,
  )
  fun findGroupManagementState(
    groupId: UUID,
    organizationId: UUID,
  ): ScimGroupManagementState?

  @Query(
    """
    SELECT id, user_id
    FROM scim_resource_mapping
    WHERE id IN (:mappingIds)
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'USER'
      AND user_active = TRUE
    """,
  )
  fun findActiveUsersByIds(
    configurationId: UUID,
    organizationId: UUID,
    mappingIds: Collection<UUID>,
  ): List<ScimActiveUserRow>

  @Query(
    """
    SELECT user_mapping.id,
           membership.user_id,
           COALESCE(NULLIF(user_mapping.attributes ->> 'displayName', ''), user_mapping.user_name) AS display
    FROM scim_resource_mapping group_mapping
    JOIN "group" managed_group
      ON managed_group.id = group_mapping.group_id
     AND managed_group.organization_id = :organizationId
    JOIN group_member membership
      ON membership.group_id = managed_group.id
    JOIN scim_resource_mapping user_mapping
      ON user_mapping.user_id = membership.user_id
     AND user_mapping.scim_configuration_id = :configurationId
     AND user_mapping.organization_id = :organizationId
     AND user_mapping.resource_type = 'USER'
     AND user_mapping.user_active = TRUE
    WHERE group_mapping.group_id = :groupId
      AND group_mapping.scim_configuration_id = :configurationId
      AND group_mapping.organization_id = :organizationId
      AND group_mapping.resource_type = 'GROUP'
    ORDER BY membership.created_at, membership.id
    """,
  )
  fun findGroupMembers(
    configurationId: UUID,
    organizationId: UUID,
    groupId: UUID,
  ): List<ScimGroupMemberRow>

  @Query(
    """
    SELECT group_mapping.group_id,
           user_mapping.id,
           membership.user_id,
           COALESCE(NULLIF(user_mapping.attributes ->> 'displayName', ''), user_mapping.user_name) AS display
    FROM scim_resource_mapping group_mapping
    JOIN "group" managed_group
      ON managed_group.id = group_mapping.group_id
     AND managed_group.organization_id = :organizationId
    JOIN group_member membership
      ON membership.group_id = managed_group.id
    JOIN scim_resource_mapping user_mapping
      ON user_mapping.user_id = membership.user_id
     AND user_mapping.scim_configuration_id = :configurationId
     AND user_mapping.organization_id = :organizationId
     AND user_mapping.resource_type = 'USER'
     AND user_mapping.user_active = TRUE
    WHERE group_mapping.group_id IN (:groupIds)
      AND group_mapping.scim_configuration_id = :configurationId
      AND group_mapping.organization_id = :organizationId
      AND group_mapping.resource_type = 'GROUP'
    ORDER BY group_mapping.created_at, group_mapping.id, membership.created_at, membership.id
    """,
  )
  fun findGroupMembersForGroups(
    configurationId: UUID,
    organizationId: UUID,
    groupIds: Collection<UUID>,
  ): List<ScimGroupPageMemberRow>

  @Query(
    """
    UPDATE scim_resource_mapping
    SET external_id = :externalId,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :id
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'GROUP'
    """,
  )
  fun updateGroup(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
    externalId: String?,
  ): Long

  @Query(
    """
    UPDATE scim_resource_mapping
    SET updated_at = CURRENT_TIMESTAMP
    WHERE group_id = :groupId
      AND organization_id = :organizationId
      AND resource_type = 'GROUP'
    """,
  )
  fun touchGroup(
    groupId: UUID,
    organizationId: UUID,
  ): Long

  @Query(
    """
    DELETE FROM scim_resource_mapping
    WHERE id = :id
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'GROUP'
    """,
  )
  fun deleteGroup(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): Long

  @Query(
    """
    SELECT * FROM scim_resource_mapping
    WHERE scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'USER'
    ORDER BY created_at, id
    """,
  )
  fun findAllUsers(
    configurationId: UUID,
    organizationId: UUID,
  ): List<ScimResourceMapping>

  @Query(
    """
    SELECT COUNT(*)
    FROM scim_resource_mapping mapping
    WHERE mapping.scim_configuration_id = :configurationId
      AND mapping.organization_id = :organizationId
      AND mapping.resource_type = 'USER'
      AND (CAST(:userName AS text) IS NULL OR lower(mapping.user_name) = lower(CAST(:userName AS text)))
      AND (CAST(:externalId AS text) IS NULL OR mapping.external_id = CAST(:externalId AS text))
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(array[:emails]::text[]) requested_email(value)
        WHERE requested_email.value IS NOT NULL
          AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(COALESCE(mapping.attributes -> 'emails', '[]'::jsonb)) email_entry
          WHERE lower(email_entry ->> 'value') = lower(requested_email.value)
        )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(array[:workEmails]::text[]) requested_email(value)
        WHERE requested_email.value IS NOT NULL
          AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(COALESCE(mapping.attributes -> 'emails', '[]'::jsonb)) email_entry
          WHERE lower(email_entry ->> 'type') = 'work'
            AND lower(email_entry ->> 'value') = lower(requested_email.value)
        )
      )
    """,
  )
  fun countUsers(
    configurationId: UUID,
    organizationId: UUID,
    userName: String?,
    externalId: String?,
    emails: List<String>,
    workEmails: List<String>,
  ): Long

  @Query(
    """
    SELECT mapping.*
    FROM scim_resource_mapping mapping
    WHERE mapping.scim_configuration_id = :configurationId
      AND mapping.organization_id = :organizationId
      AND mapping.resource_type = 'USER'
      AND (CAST(:userName AS text) IS NULL OR lower(mapping.user_name) = lower(CAST(:userName AS text)))
      AND (CAST(:externalId AS text) IS NULL OR mapping.external_id = CAST(:externalId AS text))
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(array[:emails]::text[]) requested_email(value)
        WHERE requested_email.value IS NOT NULL
          AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(COALESCE(mapping.attributes -> 'emails', '[]'::jsonb)) email_entry
          WHERE lower(email_entry ->> 'value') = lower(requested_email.value)
        )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(array[:workEmails]::text[]) requested_email(value)
        WHERE requested_email.value IS NOT NULL
          AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(COALESCE(mapping.attributes -> 'emails', '[]'::jsonb)) email_entry
          WHERE lower(email_entry ->> 'type') = 'work'
            AND lower(email_entry ->> 'value') = lower(requested_email.value)
        )
      )
    ORDER BY mapping.created_at, mapping.id
    LIMIT :limit OFFSET :offset
    """,
  )
  fun findUsersPage(
    configurationId: UUID,
    organizationId: UUID,
    userName: String?,
    externalId: String?,
    emails: List<String>,
    workEmails: List<String>,
    offset: Long,
    limit: Int,
  ): List<ScimResourceMapping>

  @Query(
    """
    SELECT * FROM scim_resource_mapping
    WHERE id = :id
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'USER'
    """,
  )
  fun findUser(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): ScimResourceMapping?

  @Query(
    """
    SELECT * FROM scim_resource_mapping
    WHERE user_id = :userId
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'USER'
    """,
  )
  fun findUserByUserId(
    userId: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): ScimResourceMapping?

  @Query(
    """
    SELECT * FROM scim_resource_mapping
    WHERE user_id = :userId
      AND organization_id = :organizationId
      AND resource_type = 'USER'
    FOR UPDATE
    """,
  )
  fun findUserByUserIdAndOrganizationIdForUpdate(
    userId: UUID,
    organizationId: UUID,
  ): ScimResourceMapping?

  @Query(
    """
    SELECT * FROM scim_resource_mapping
    WHERE id = :id
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'USER'
    FOR UPDATE
    """,
  )
  fun findUserForUpdate(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): ScimResourceMapping?

  @Query(
    """
    UPDATE scim_resource_mapping
    SET user_name = :userName,
        primary_email = :primaryEmail,
        external_id = :externalId,
        user_active = :active,
        attributes = CAST(:attributesJson AS jsonb),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :id
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'USER'
    """,
  )
  fun updateUser(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
    userName: String,
    primaryEmail: String,
    externalId: String?,
    active: Boolean,
    attributesJson: String,
  ): Long

  @Query(
    """
    DELETE FROM scim_resource_mapping
    WHERE id = :id
      AND scim_configuration_id = :configurationId
      AND organization_id = :organizationId
      AND resource_type = 'USER'
    """,
  )
  fun deleteUser(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): Long

  @Query(
    """
    SELECT group_mapping.id AS id, managed_group.name AS display_name
    FROM scim_resource_mapping group_mapping
    JOIN "group" managed_group
      ON managed_group.id = group_mapping.group_id
     AND managed_group.organization_id = :organizationId
    JOIN group_member membership
      ON membership.group_id = managed_group.id
    WHERE group_mapping.scim_configuration_id = :configurationId
      AND group_mapping.organization_id = :organizationId
      AND group_mapping.resource_type = 'GROUP'
      AND membership.user_id = :userId
    ORDER BY group_mapping.created_at, group_mapping.id
    """,
  )
  fun findGroupsForUser(
    configurationId: UUID,
    organizationId: UUID,
    userId: UUID,
  ): List<ScimUserGroupRow>

  @Query(
    """
    SELECT membership.user_id AS user_id,
           group_mapping.id AS id,
           managed_group.name AS display_name
    FROM scim_resource_mapping group_mapping
    JOIN "group" managed_group
      ON managed_group.id = group_mapping.group_id
     AND managed_group.organization_id = :organizationId
    JOIN group_member membership
      ON membership.group_id = managed_group.id
    WHERE group_mapping.scim_configuration_id = :configurationId
      AND group_mapping.organization_id = :organizationId
      AND group_mapping.resource_type = 'GROUP'
      AND membership.user_id IN (:userIds)
    ORDER BY membership.user_id, group_mapping.created_at, group_mapping.id
    """,
  )
  fun findGroupsForUsers(
    configurationId: UUID,
    organizationId: UUID,
    userIds: List<UUID>,
  ): List<ScimUserGroupMembershipRow>
}

@Introspected
data class ScimUserGroupRow(
  val id: UUID,
  val displayName: String,
)

@Introspected
data class ScimGroupRow(
  val id: UUID,
  val scimConfigurationId: UUID,
  val organizationId: UUID,
  val groupId: UUID,
  val externalId: String?,
  val displayName: String,
  val createdAt: OffsetDateTime,
  val updatedAt: OffsetDateTime,
)

@Introspected
data class ScimGroupManagementState(
  val scimConfigurationId: UUID,
  val enabled: Boolean,
)

@Introspected
data class ScimActiveUserRow(
  val id: UUID,
  val userId: UUID,
)

@Introspected
data class ScimGroupMemberRow(
  val id: UUID,
  val userId: UUID,
  val display: String,
)

@Introspected
data class ScimGroupPageMemberRow(
  val groupId: UUID,
  val id: UUID,
  val userId: UUID,
  val display: String,
)

@Introspected
data class ScimUserGroupMembershipRow(
  val userId: UUID,
  val id: UUID,
  val displayName: String,
)
