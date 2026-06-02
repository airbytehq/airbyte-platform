/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dsr

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import io.micronaut.core.annotation.Introspected
import java.util.UUID

/**
 * Snapshot of every resource that will be hard-deleted as part of a GDPR / DSR deletion for a
 * single user.
 *
 * The manifest is captured by the read-only preview endpoint and stored verbatim on the
 * `data_subject_deletion_request` row. Execute rebuilds the current manifest, compares destructive
 * scopes against this preview snapshot, and only proceeds when the reviewed scope still matches.
 */
@Introspected
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class DsrManifest(
  /** The email address supplied by DataGrail / Support. */
  val targetEmail: String,
  /** The DataGrail request identifier used as the final user-row tombstone value. */
  val datagrailId: String,
  /** The Airbyte user being deleted. Kept as a top-level field for deletion SQL convenience. */
  val userId: UUID,
  /** Human-readable user details from the Airbyte `user` row. */
  val user: ManifestUser?,
  /** Workspaces owned by the user (matched by `workspace.email = user.email`). */
  val workspaceIds: List<UUID>,
  /** Workspace IDs and names for Support review. */
  val workspaceRefs: List<ManifestWorkspace>,
  /** Organizations owned by the user (matched by `organization.email = user.email`). */
  val organizationIds: List<UUID>,
  /** Organization IDs and names for Support review. */
  val organizationRefs: List<ManifestOrganization>,
  /** Connection IDs across the deleted workspaces (used to scope `jobs.scope` deletion). */
  val connectionIds: List<UUID>,
  /** Connection IDs with source/destination labels for Support review. */
  val connectionRefs: List<ManifestConnection>,
  /** Source actor IDs (`actor.actor_type='source'`) across the deleted workspaces. */
  val sourceIds: List<UUID>,
  /** Destination actor IDs (`actor.actor_type='destination'`) across the deleted workspaces. */
  val destinationIds: List<UUID>,
  /** Connector-builder project IDs in the deleted workspaces. */
  val connectorBuilderProjectIds: List<UUID>,
  /** Permission IDs belonging to the user (may include perms in other orgs/workspaces). */
  val permissionIds: List<UUID>,
  /** External (Keycloak / Firebase) auth identities mapped to this user. */
  val authUsers: List<ManifestAuthUser>,
  /** Number of `jobs` rows that would be hard-deleted for the captured connection scopes. */
  val jobCount: Long,
  /** Number of `attempts` rows that would be hard-deleted for the captured connection scopes. */
  val attemptCount: Long,
  /** Keycloak users found by exact email search in the cloud users realm. */
  val keycloakUsers: List<ManifestKeycloakUser>,
  /** Running connection-manager workflows that execute will terminate. */
  val temporalWorkflows: List<ManifestTemporalWorkflow>,
) {
  @Introspected
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
  data class ManifestUser(
    val userId: UUID,
    val email: String?,
    val name: String?,
  )

  @Introspected
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
  data class ManifestWorkspace(
    val workspaceId: UUID,
    val name: String?,
  )

  @Introspected
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
  data class ManifestOrganization(
    val organizationId: UUID,
    val name: String?,
  )

  @Introspected
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
  data class ManifestConnection(
    val connectionId: UUID,
    val sourceId: UUID?,
    val sourceName: String?,
    val destinationId: UUID?,
    val destinationName: String?,
  )

  @Introspected
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
  data class ManifestAuthUser(
    val authUserId: String,
    val authProvider: String?,
  )

  @Introspected
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
  data class ManifestKeycloakUser(
    val authUserId: String,
    val email: String?,
    val username: String?,
    val enabled: Boolean?,
  )

  @Introspected
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
  data class ManifestTemporalWorkflow(
    val workflowId: String,
    val connectionId: UUID,
    val running: Boolean,
  )

  @get:com.fasterxml.jackson.annotation.JsonIgnore
  val permissionCount: Int
    get() = permissionIds.size
}
