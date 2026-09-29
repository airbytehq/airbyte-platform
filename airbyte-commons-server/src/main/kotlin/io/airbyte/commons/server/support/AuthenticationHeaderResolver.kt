/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import com.fasterxml.jackson.core.type.TypeReference
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.AIRBYTE_USER_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONFIG_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_IDS_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CREATOR_USER_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DATAPLANE_GROUP_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DATAPLANE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.IS_PUBLIC_API_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_ALT_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_SNAKE_CASE_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.PERMISSION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_TYPE_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER
import io.airbyte.config.ScopeType
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Resolves the workspaces and organizations a request refers to from the headers that
 * [AuthorizationServerHandler] populated out of the request body.
 *
 * Every identifier in the request must resolve to the same workspace and organization. A request that names
 * identifiers from different scopes, or an identifier that cannot be resolved, resolves to no scope at all and
 * therefore receives no roles.
 */
@Singleton
class AuthenticationHeaderResolver(
  private val workspaceHelper: WorkspaceHelper,
  private val permissionHandler: PermissionHandler,
  private val userPersistence: UserPersistence?,
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataplaneService: DataplaneService,
  private val metricClient: MetricClient,
) {
  /**
   * The scope a request refers to. [workspaceOrganizations] maps each resolved workspace to its organization.
   */
  data class AuthScope(
    val workspaceIds: List<UUID>,
    val organizationIds: List<UUID>,
    val workspaceOrganizations: Map<UUID, UUID>,
  )

  /**
   * Resolve corresponding organization ID based on headers that were set by the AuthorizationServerHandler
   */
  @Nullable
  fun resolveOrganization(properties: Map<String, String>): List<UUID>? {
    val scope = resolveScope(properties, report = false) ?: return null
    return scope.organizationIds.ifEmpty { null }
  }

  /**
   * Resolves workspaces from header.
   */
  @Nullable // This is an indication that the workspace ID as a group for auth needs refactoring
  fun resolveWorkspace(properties: Map<String, String>): List<UUID>? {
    val scope = resolveScope(properties, report = false) ?: return null
    if (scope.workspaceIds.isEmpty()) {
      // A public API request that names no workspace passes through so the handler can scope to the caller's
      // workspaces or fail on its own.
      return if (!properties.containsKey(WORKSPACE_IDS_HEADER) && properties.containsKey(IS_PUBLIC_API_HEADER)) {
        emptyList()
      } else {
        null
      }
    }
    return scope.workspaceIds
  }

  /**
   * Resolves the workspaces and organizations the request refers to.
   *
   * [trustedWorkspaceOrganizations] are workspace-to-organization pairs the caller loaded fresh; they take precedence
   * over the cached mapping for those workspaces.
   *
   * Returns null when the identifiers in the request disagree or one of them cannot be resolved. A denial is logged
   * and counted only when [report] is set; the wrappers above resolve silently because [RoleResolver] reports the
   * authoritative resolution for the request.
   */
  fun resolveScope(
    properties: Map<String, String>,
    trustedWorkspaceOrganizations: Map<UUID, UUID> = emptyMap(),
    report: Boolean = true,
  ): AuthScope? {
    try {
      // Identifiers that each name exactly one workspace. They must all agree, and the check below enforces that by
      // requiring this set to hold at most one entry.
      val single = linkedSetOf<UUID>()
      properties[WORKSPACE_ID_HEADER]?.let { single += UUID.fromString(it) }
      properties[CONNECTION_ID_HEADER]?.let { single += workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(it)) }
      val sourceId = properties[SOURCE_ID_HEADER]
      val destinationId = properties[DESTINATION_ID_HEADER]
      if (sourceId != null && destinationId != null) {
        single += workspaceHelper.getWorkspaceForConnection(UUID.fromString(sourceId), UUID.fromString(destinationId))
      } else {
        sourceId?.let { single += workspaceHelper.getWorkspaceForSourceId(UUID.fromString(it)) }
        destinationId?.let { single += workspaceHelper.getWorkspaceForDestinationId(UUID.fromString(it)) }
      }
      properties[OPERATION_ID_HEADER]?.let { single += workspaceHelper.getWorkspaceForOperationId(UUID.fromString(it)) }
      properties[CONFIG_ID_HEADER]?.let { single += workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(it)) }
      // A body field named `id` is a job id unless it is a UUID, which other endpoints use for an id of their own.
      // A value that is neither is denied, because the endpoint can still read it as a number.
      properties[JOB_ID_HEADER]?.let { id ->
        if (runCatching { UUID.fromString(id) }.isFailure) {
          single += workspaceHelper.getWorkspaceForJobId(id.toLong())
        }
      }
      properties[JOB_ID_ALT_HEADER]?.let { single += workspaceHelper.getWorkspaceForJobId(it.toLong()) }
      val scopeType = properties[SCOPE_TYPE_HEADER]
      val scopeId = properties[SCOPE_ID_HEADER]
      val scopeIsWorkspace = scopeType.equals(ScopeType.WORKSPACE.value(), ignoreCase = true)
      val scopeIsOrganization = scopeType.equals(ScopeType.ORGANIZATION.value(), ignoreCase = true)
      // A scope id with a scope type that is neither of the two is denied, because the endpoint can still read it
      // as one of them.
      if (scopeId != null && scopeType != null && !scopeIsWorkspace && !scopeIsOrganization) {
        return denied("unresolvable_ref", properties, report, "scopeType")
      }
      if (scopeId != null && scopeIsWorkspace) {
        single += UUID.fromString(scopeId)
      }
      // permissions/create dual-writes a client-generated permissionId that does not exist yet. A permission that
      // does not exist constrains nothing, so it is ignored. A permission id that is not a UUID is denied.
      val permission =
        properties[PERMISSION_ID_HEADER]?.let {
          val permissionId = UUID.fromString(it)
          try {
            permissionHandler.getPermissionById(permissionId)
          } catch (e: ConfigNotFoundException) {
            log.debug(e) { "Ignoring permission id that does not exist." }
            null
          }
        }
      permission?.workspaceId?.let { single += it }
      if (single.size > 1) {
        return denied("workspace_refs_disagree", properties, report)
      }

      // The workspaces of the listed connections. These may span workspaces, so they are a list, not a set, and they
      // only have to agree with the single workspace when the request names one.
      val connectionIds = properties[CONNECTION_IDS_HEADER]?.let { parseUuidList(it) }
      if (connectionIds != null && connectionIds.size > MAX_SCOPE_LIST_SIZE) {
        return denied("too_many_connection_ids", properties, report)
      }
      val fromConnectionIds =
        connectionIds
          ?.distinct()
          ?.map { workspaceHelper.getWorkspaceForConnectionId(it) }
          .orEmpty()
      val workspace = single.firstOrNull()
      if (workspace != null && fromConnectionIds.any { it != workspace }) {
        return denied("connection_ids_disagree", properties, report)
      }

      // A handler given `workspaceIds` acts on every listed workspace, so the caller needs a role on each of them; a
      // single resource inside the list must not narrow the authorized set to its own workspace.
      val listed = properties[WORKSPACE_IDS_HEADER]?.let { parseUuidList(it) }
      if (listed != null && listed.size > MAX_SCOPE_LIST_SIZE) {
        return denied("too_many_workspace_ids", properties, report)
      }
      if (listed != null && (single + fromConnectionIds).any { it !in listed }) {
        return denied("outside_workspace_ids", properties, report)
      }

      // Organizations named directly, through a scope, a permission or the owner of a region. They must all agree
      // too, and the check below enforces that the same way: at most one entry.
      val organizationClaims = linkedSetOf<UUID>()
      if (scopeId != null && scopeIsOrganization) {
        organizationClaims += UUID.fromString(scopeId)
      }
      permission?.organizationId?.let { organizationClaims += it }
      properties[ORGANIZATION_ID_HEADER]?.let { organizationClaims += UUID.fromString(it) }
      properties[ORGANIZATION_ID_SNAKE_CASE_HEADER]?.let { organizationClaims += UUID.fromString(it) }
      val namesOrganization = organizationClaims.isNotEmpty()
      properties[DATAPLANE_GROUP_ID_HEADER]?.let {
        val owner = resolveOrganizationIdFromDataplaneGroupHeader(it) ?: return denied("unresolvable_dataplane_group", properties, report)
        addRegionOwnerClaim(organizationClaims, owner, namesOrganization)
      }
      properties[DATAPLANE_ID_HEADER]?.let {
        val owner = resolveOrganizationIdFromDataplaneHeader(it) ?: return denied("unresolvable_dataplane", properties, report)
        addRegionOwnerClaim(organizationClaims, owner, namesOrganization)
      }
      if (organizationClaims.size > 1) {
        return denied("organization_refs_disagree", properties, report)
      }

      // The workspaces the caller needs a role on: the list when one is given, otherwise the single workspace,
      // otherwise the workspaces of the listed connections.
      val workspaceIds: List<UUID> =
        when {
          listed != null -> listed
          workspace != null -> listOf(workspace)
          else -> fromConnectionIds
        }
      // A named organization must be the organization of every workspace named. Without one, the workspaces'
      // organizations are the scope.
      val workspaceOrganizations =
        organizationsFor(workspaceIds.toSet(), trustedWorkspaceOrganizations, organizationClaims)
          ?: return denied("unresolvable_workspace_organization", properties, report)
      val workspaceOrganizationIds = workspaceOrganizations.values.toSet()
      if (organizationClaims.isNotEmpty() && workspaceOrganizationIds.isNotEmpty() && organizationClaims != workspaceOrganizationIds) {
        return denied("organization_workspace_mismatch", properties, report)
      }
      val organizationIds = if (organizationClaims.isNotEmpty()) organizationClaims.toList() else workspaceOrganizationIds.toList()
      return AuthScope(workspaceIds, organizationIds, workspaceOrganizations)
    } catch (e: Exception) {
      if (!isUnresolvable(e)) {
        throw e
      }
      log.debug(e) { "Unable to resolve an authorization scope identifier." }
      return denied("unresolvable_ref", properties, report, e::class.simpleName)
    }
  }

  /**
   * Maps each workspace to its organization. Trusted pairs win over the cache.
   *
   * When the request carries an organization claim, every workspace's organization must be known so the claim can
   * be verified, so an unknown workspace yields null. Without a claim the mapping only feeds organization-implied
   * roles, so an unknown workspace is skipped. A lookup that fails for any other reason propagates.
   */
  private fun organizationsFor(
    workspaceIds: Set<UUID>,
    trustedWorkspaceOrganizations: Map<UUID, UUID>,
    organizationClaims: Set<UUID>,
  ): Map<UUID, UUID>? {
    val organizations = mutableMapOf<UUID, UUID>()
    for (workspaceId in workspaceIds) {
      val trusted = trustedWorkspaceOrganizations[workspaceId]
      if (trusted != null) {
        organizations[workspaceId] = trusted
        continue
      }
      try {
        organizations[workspaceId] = workspaceHelper.getOrganizationForWorkspace(workspaceId)
      } catch (e: RuntimeException) {
        if (!isUnresolvable(e)) {
          throw e
        }
        log.debug(e) { "Unable to resolve organization ID for workspace ID: $workspaceId" }
        if (organizationClaims.isNotEmpty()) {
          return null
        }
      }
    }
    return organizations
  }

  /**
   * True when the failure means the identifier names no existing resource, as opposed to the lookup itself failing.
   * The whole cause chain is inspected because [WorkspaceHelper.getOrganizationForWorkspace] wraps its cause.
   */
  private fun isUnresolvable(e: Throwable): Boolean =
    generateSequence(e) { it.cause }.any {
      it is IllegalArgumentException ||
        it is JsonValidationException ||
        it is ConfigNotFoundException ||
        it is io.airbyte.config.persistence.ConfigNotFoundException
    }

  /**
   * The default organization's regions and the PrivateLink regions are shared pools that every entitled organization
   * may use. When the request already names its organization, the pool owner would only contradict it, and the
   * handler checks that the organization may use the region. Without a named organization the pool owner is the
   * request's scope, so a shared region cannot be reached through a decoy workspace.
   */
  private fun addRegionOwnerClaim(
    organizationClaims: MutableSet<UUID>,
    owner: UUID,
    namesOrganization: Boolean,
  ) {
    if (owner !in SHARED_REGION_OWNERS || !namesOrganization) {
      organizationClaims += owner
    }
  }

  private fun denied(
    reason: String,
    properties: Map<String, String>,
    report: Boolean,
    detail: String? = null,
  ): AuthScope? {
    if (!report) {
      return null
    }
    val refs = SCOPE_HEADERS.filter { properties.containsKey(it) }.joinToString(", ")
    val cause = if (detail != null) "$reason: $detail" else reason
    log.warn { "Denied authorization scope resolution ($cause): presentFields=[$refs]" }
    metricClient.count(OssMetricsRegistry.AUTHORIZATION_SCOPE_CONFLICT, 1L, MetricAttribute(MetricTags.FAILURE_TYPE, reason))
    return null
  }

  private fun parseUuidList(raw: String): List<UUID> =
    Jsons
      .deserialize(raw, object : TypeReference<List<String>>() {})
      .map { UUID.fromString(it) }

  /**
   * Resolves the auth user ids of the user that the request names.
   *
   * Every user id in the request must name the same user, so the result is what all of them have in common. It is
   * empty when they name different users.
   */
  @Nullable
  fun resolveAuthUserIds(properties: Map<String?, String?>): Set<String>? {
    log.debug { "properties: $properties" }
    try {
      val named =
        listOfNotNull(
          properties[EXTERNAL_AUTH_ID_HEADER]?.let { setOf(it) },
          properties[AIRBYTE_USER_ID_HEADER]?.let { resolveAirbyteUserIdToAuthUserIds(it) },
          properties[CREATOR_USER_ID_HEADER]?.let { resolveAirbyteUserIdToAuthUserIds(it) },
        )
      if (named.isEmpty()) {
        log.debug { "Request does not contain any headers that resolve to a user ID." }
        return null
      }
      return named.reduce { agreed, next -> agreed intersect next }
    } catch (e: Exception) {
      log.debug(e) { "Unable to resolve user ID." }
      return null
    }
  }

  private fun resolveAirbyteUserIdToAuthUserIds(airbyteUserId: String): Set<String> {
    val authUserIds = userPersistence?.listAuthUserIdsForUser(UUID.fromString(airbyteUserId)) ?: emptySet()

    require(!authUserIds.isEmpty()) { String.format("Could not find any authUserIds for userId %s", airbyteUserId) }

    return HashSet(authUserIds)
  }

  private fun resolveOrganizationIdFromDataplaneGroupHeader(dataplaneGroupHeaderValue: String?): UUID? {
    if (dataplaneGroupHeaderValue == null) {
      return null
    }
    return try {
      val dataplaneGroupId = UUID.fromString(dataplaneGroupHeaderValue)
      dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId)
    } catch (e: Exception) {
      log.debug("Unable to resolve organization ID from dataplane group header.", e)
      null
    }
  }

  private fun resolveOrganizationIdFromDataplaneHeader(dataplaneHeaderValue: String?): UUID? {
    if (dataplaneHeaderValue == null) {
      return null
    }
    return try {
      val dataplaneId = UUID.fromString(dataplaneHeaderValue)
      val dataplaneGroupId = dataplaneService.getDataplane(dataplaneId).dataplaneGroupId
      dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId)
    } catch (e: Exception) {
      log.debug("Unable to resolve organization ID from dataplane header.", e)
      null
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
    private const val MAX_SCOPE_LIST_SIZE = 1000

    // Region pools that belong to no single organization. Handlers apply the entitlement checks for these owners;
    // here they only mean the region does not pin the request to an organization.
    private val SHARED_REGION_OWNERS = setOf(DEFAULT_ORGANIZATION_ID, PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID)

    // Every header the request scanner can populate, for the denial log line.
    private val SCOPE_HEADERS = AuthenticationId.entries.map { it.httpHeader }.distinct()
  }
}
