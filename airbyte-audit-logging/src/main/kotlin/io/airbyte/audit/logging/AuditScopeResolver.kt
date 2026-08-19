/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER
import io.airbyte.data.helpers.WorkspaceHelper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpHeaders
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Resolved organization/workspace attribution for a single audit log entry.
 *
 * [workspaceId] is populated whenever the audited action has a workspace context; genuinely
 * organization-level actions (e.g. SSO/SCIM config, organization updates, domain verification)
 * carry a null workspaceId.
 */
data class ResolvedAuditScope(
  val organizationId: UUID? = null,
  val workspaceId: UUID? = null,
)

/**
 * Resolves the organization/workspace scope used to attribute an audit log entry.
 *
 * Resolution order:
 * - workspaceId: `X-Airbyte-Workspace-Id` header -> `workspaceId` field in the request/response
 *   body -> resource lookup when the body only carries a `connectionId`/`sourceId`/`destinationId`
 *   (the resource's workspace is used) -> null.
 * - organizationId: `X-Airbyte-Organization-Id` header -> `organizationId` field in the
 *   request/response body -> the organization of the resolved workspaceId -> null (the entry is
 *   written under the `unknown` organization partition and is considered internal-only).
 *
 * Resolution never throws: any failure to read a header, parse a body field, or look up a resource
 * falls through to the next source so that attribution problems never break the audited request.
 * Only the identifier fields above are read from the bodies and only the resolved ids are stamped
 * on the entry; no body content is persisted here, so provider-side redaction (e.g. the SSO secret
 * allowlist) is preserved.
 */
@Singleton
class AuditScopeResolver(
  private val workspaceHelper: WorkspaceHelper,
) {
  /**
   * Resolves the attribution scope for the given request headers and request/response bodies.
   * The response body is null when the audited request failed.
   */
  fun resolveScope(
    headers: HttpHeaders?,
    requestBody: Any?,
    responseBody: Any?,
  ): ResolvedAuditScope {
    val workspaceId = resolveWorkspaceId(headers, requestBody, responseBody)
    val organizationId = resolveOrganizationId(headers, requestBody, responseBody, workspaceId)
    return ResolvedAuditScope(organizationId = organizationId, workspaceId = workspaceId)
  }

  private fun resolveWorkspaceId(
    headers: HttpHeaders?,
    requestBody: Any?,
    responseBody: Any?,
  ): UUID? =
    uuidFromHeader(headers, WORKSPACE_ID_HEADER)
      ?: uuidFromBodyField(WORKSPACE_ID_FIELD, requestBody, responseBody)
      ?: resolveWorkspaceFromResourceIds(requestBody)
      ?: resolveWorkspaceFromResourceIds(responseBody)

  private fun resolveOrganizationId(
    headers: HttpHeaders?,
    requestBody: Any?,
    responseBody: Any?,
    workspaceId: UUID?,
  ): UUID? =
    uuidFromHeader(headers, ORGANIZATION_ID_HEADER)
      ?: uuidFromBodyField(ORGANIZATION_ID_FIELD, requestBody, responseBody)
      ?: workspaceId?.let { resolveOrganizationForWorkspace(it) }

  private fun resolveOrganizationForWorkspace(workspaceId: UUID): UUID? =
    try {
      workspaceHelper.getOrganizationForWorkspace(workspaceId)
    } catch (e: Exception) {
      logger.debug(e) { "Unable to resolve organization for workspace $workspaceId for audit logging." }
      null
    }

  private fun resolveWorkspaceFromResourceIds(body: Any?): UUID? {
    val node = toJsonNode(body) ?: return null
    uuidFromField(node, CONNECTION_ID_FIELD)?.let { connectionId ->
      lookupWorkspace { workspaceHelper.getWorkspaceForConnectionId(connectionId) }?.let { return it }
    }
    uuidFromField(node, SOURCE_ID_FIELD)?.let { sourceId ->
      lookupWorkspace { workspaceHelper.getWorkspaceForSourceId(sourceId) }?.let { return it }
    }
    uuidFromField(node, DESTINATION_ID_FIELD)?.let { destinationId ->
      lookupWorkspace { workspaceHelper.getWorkspaceForDestinationId(destinationId) }?.let { return it }
    }
    return null
  }

  private fun lookupWorkspace(lookup: () -> UUID): UUID? =
    try {
      lookup()
    } catch (e: Exception) {
      logger.debug(e) { "Unable to resolve workspace from audit-logged body resource id." }
      null
    }

  private fun uuidFromHeader(
    headers: HttpHeaders?,
    headerName: String,
  ): UUID? = headers?.get(headerName)?.let { parseUuid(it) }

  private fun uuidFromBodyField(
    field: String,
    vararg bodies: Any?,
  ): UUID? = bodies.firstNotNullOfOrNull { body -> toJsonNode(body)?.let { node -> uuidFromField(node, field) } }

  private fun uuidFromField(
    node: JsonNode,
    field: String,
  ): UUID? =
    if (node.hasNonNull(field)) {
      parseUuid(node.get(field).asText())
    } else {
      null
    }

  private fun parseUuid(value: String?): UUID? =
    value
      ?.takeIf { it.isNotBlank() }
      ?.let {
        try {
          UUID.fromString(it)
        } catch (e: IllegalArgumentException) {
          logger.debug(e) { "Unable to parse audit scope id value as UUID: $it" }
          null
        }
      }

  private fun toJsonNode(body: Any?): JsonNode? =
    try {
      body?.let { Jsons.jsonNode(it) }
    } catch (e: Exception) {
      logger.debug(e) { "Unable to convert audit-logged body to JSON for scope resolution." }
      null
    }

  companion object {
    private val logger = KotlinLogging.logger {}
    private const val WORKSPACE_ID_FIELD = "workspaceId"
    private const val ORGANIZATION_ID_FIELD = "organizationId"
    private const val CONNECTION_ID_FIELD = "connectionId"
    private const val SOURCE_ID_FIELD = "sourceId"
    private const val DESTINATION_ID_FIELD = "destinationId"
  }
}
