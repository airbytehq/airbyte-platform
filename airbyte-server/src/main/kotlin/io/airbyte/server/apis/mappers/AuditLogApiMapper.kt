/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.mappers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.AuditLogActor
import io.airbyte.api.server.generated.models.AuditLogListRequestBody
import io.airbyte.api.server.generated.models.AuditLogRead
import io.airbyte.api.server.generated.models.AuditLogReadList
import io.airbyte.audit.logging.model.Actor
import io.airbyte.audit.logging.model.AuditLogEntry
import io.airbyte.audit.logging.read.AuditLogPage
import io.airbyte.audit.logging.read.AuditLogQuery
import io.airbyte.commons.json.Jsons

/**
 * Maps between the generated audit-log API models and the audit-logging module's service models.
 */
object AuditLogApiMapper {
  fun toQuery(body: AuditLogListRequestBody): AuditLogQuery =
    AuditLogQuery.of(
      organizationId = body.organizationId,
      startTime = body.startTime?.toInstant(),
      endTime = body.endTime?.toInstant(),
      workspaceId = body.workspaceId,
      actorId = body.actorId,
      operation = body.operation,
      success = body.success,
      searchText = body.searchText,
      pageSize = body.pageSize,
      pageToken = body.pageToken,
    )

  fun toApi(page: AuditLogPage): AuditLogReadList =
    AuditLogReadList(
      auditLogs = page.entries.map { toApi(it) },
      nextPageToken = page.nextPageToken,
    )

  private fun toApi(entry: AuditLogEntry): AuditLogRead =
    AuditLogRead(
      id = entry.id,
      timestamp = entry.timestamp,
      actor = entry.actor?.let { toApi(it) },
      operation = entry.operation,
      request = toJsonNode(entry.request),
      response = toJsonNode(entry.response),
      success = entry.success,
      errorMessage = entry.errorMessage,
      organizationId = entry.organizationId,
      workspaceId = entry.workspaceId,
    )

  private fun toApi(actor: Actor): AuditLogActor =
    AuditLogActor(
      actorId = actor.actorId,
      email = actor.email,
      ipAddress = actor.ipAddress,
      userAgent = actor.userAgent,
    )

  /**
   * Audit providers store request/response summaries as serialized JSON strings; parse them back
   * into JSON nodes so API consumers receive structured content. Values that are not parseable
   * JSON are returned as plain text nodes.
   */
  private fun toJsonNode(value: Any?): JsonNode? =
    value?.let {
      val node = Jsons.jsonNode(it)
      try {
        Jsons.deserializeIfText(node)
      } catch (e: Exception) {
        node
      }
    }
}
