/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.OperatorType
import org.jooq.DSLContext
import org.jooq.impl.DSL
import java.util.UUID

class WebhookConfigInUseException :
  IllegalStateException(
    "Remove connection transformations using this webhook configuration before deleting it. To rotate the token, save a replacement instead.",
  )

class WebhookConfigNotFoundException :
  IllegalArgumentException("Webhook configuration no longer exists in this workspace. Reload and select a configuration.")

/** Queries used inside workspace and operation write transactions. */
internal object WorkspaceWebhookConfigRepository {
  fun validateConnectionOperationUpdate(
    ctx: DSLContext,
    connectionId: UUID,
    operationIds: Set<UUID>,
  ): Set<UUID> {
    if (operationIds.isEmpty()) return existingOperationIds(ctx, connectionId)

    val sourceId =
      ctx
        .select(Tables.CONNECTION.SOURCE_ID)
        .from(Tables.CONNECTION)
        .where(Tables.CONNECTION.ID.eq(connectionId))
        .fetchOne()
        ?.value1()
        ?: throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC, connectionId)
    return validateConnectionOperations(ctx, sourceId, connectionId, operationIds)
  }

  fun validateConnectionOperations(
    ctx: DSLContext,
    sourceId: UUID,
    connectionId: UUID,
    operationIds: Set<UUID>,
  ): Set<UUID> {
    if (operationIds.isEmpty()) return existingOperationIds(ctx, connectionId)

    val row =
      ctx
        .select(Tables.WORKSPACE.ID, Tables.WORKSPACE.ORGANIZATION_ID)
        .from(Tables.ACTOR)
        .join(Tables.WORKSPACE)
        .on(Tables.ACTOR.WORKSPACE_ID.eq(Tables.WORKSPACE.ID))
        .where(Tables.ACTOR.ID.eq(sourceId))
        .fetchOne()
        ?: throw ConfigNotFoundException(ConfigNotFoundType.SOURCE_CONNECTION, sourceId)
    val workspace = StandardWorkspace().withWorkspaceId(row.value1()).withOrganizationId(row.value2())
    val configs = getForUpdate(ctx, workspace)
    val existingOperationIds = existingOperationIds(ctx, connectionId)
    val operations =
      ctx
        .selectFrom(Tables.OPERATION)
        .where(Tables.OPERATION.WORKSPACE_ID.eq(workspace.workspaceId))
        .and(Tables.OPERATION.ID.`in`(operationIds))
        .fetch()
    val missingOperationIds = operationIds - operations.map { it.id }.toSet()
    if (missingOperationIds.isNotEmpty()) {
      throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC_OPERATION, missingOperationIds.first())
    }

    val addedOperationIds = operationIds - existingOperationIds
    val addedWebhookOperations = operations.filter { it.id in addedOperationIds && it.operatorType == OperatorType.webhook }

    val configIds = configs.path("webhookConfigs").map { it.path("id").asText() }.toSet()

    for (operation in addedWebhookOperations) {
      val configId = operation.operatorWebhook?.data()?.let { Jsons.deserialize(it).path("webhookConfigId").asText() }
      if (configId !in configIds) throw WebhookConfigNotFoundException()
    }
    return existingOperationIds
  }

  private fun existingOperationIds(
    ctx: DSLContext,
    connectionId: UUID,
  ): Set<UUID> =
    ctx
      .select(Tables.CONNECTION_OPERATION.OPERATION_ID)
      .from(Tables.CONNECTION_OPERATION)
      .where(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
      .fetchSet(Tables.CONNECTION_OPERATION.OPERATION_ID)

  fun isOperationAttached(
    ctx: DSLContext,
    operationId: UUID,
  ): Boolean = ctx.fetchExists(Tables.CONNECTION_OPERATION, Tables.CONNECTION_OPERATION.OPERATION_ID.eq(operationId))

  fun getForUpdate(
    ctx: DSLContext,
    workspace: StandardWorkspace,
  ): JsonNode {
    val row =
      ctx
        .select(Tables.WORKSPACE.WEBHOOK_OPERATION_CONFIGS)
        .from(Tables.WORKSPACE)
        .where(Tables.WORKSPACE.ID.eq(workspace.workspaceId))
        .and(Tables.WORKSPACE.ORGANIZATION_ID.eq(workspace.organizationId))
        .forUpdate()
        .fetchOne()
        ?: throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, workspace.workspaceId)
    return row.value1()?.data()?.let { Jsons.deserialize(it) } ?: Jsons.emptyObject()
  }

  fun validateRemoval(
    ctx: DSLContext,
    workspace: StandardWorkspace,
    previousConfigs: JsonNode,
  ) {
    val retainedIds =
      workspace.webhookOperationConfigs
        ?.path("webhookConfigs")
        ?.map { it.path("id").asText() }
        ?.toSet()
        .orEmpty()
    val removedIds = previousConfigs.path("webhookConfigs").map { it.path("id").asText() }.toSet() - retainedIds
    if (removedIds.isEmpty()) return

    // Include deprecated connections and attached tombstones: queued or running jobs may still invoke these operations.
    val webhookConfigId = DSL.field("{0}->>'webhookConfigId'", String::class.java, Tables.OPERATION.OPERATOR_WEBHOOK)
    if (
      ctx.fetchExists(
        DSL
          .selectOne()
          .from(Tables.OPERATION)
          .where(Tables.OPERATION.WORKSPACE_ID.eq(workspace.workspaceId))
          .and(
            DSL.exists(
              DSL
                .selectOne()
                .from(Tables.CONNECTION_OPERATION)
                .where(Tables.CONNECTION_OPERATION.OPERATION_ID.eq(Tables.OPERATION.ID)),
            ),
          ).and(Tables.OPERATION.OPERATOR_TYPE.eq(OperatorType.webhook))
          .and(webhookConfigId.`in`(removedIds)),
      )
    ) {
      throw WebhookConfigInUseException()
    }
  }
}
