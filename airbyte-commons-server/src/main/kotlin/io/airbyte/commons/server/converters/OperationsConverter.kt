/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.OperationRead
import io.airbyte.api.server.generated.models.OperatorConfiguration
import io.airbyte.api.server.generated.models.OperatorType
import io.airbyte.api.server.generated.models.OperatorWebhook
import io.airbyte.api.server.generated.models.OperatorWebhookDbtCloud
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.WebhookOperationConfigs
import java.util.Optional
import java.util.regex.Pattern

/**
 * Convert between API and internal versions of operations models.
 */
object OperationsConverter {
  const val DBT_CLOUD_WEBHOOK_TYPE: String = "dbt cloud"

  @JvmStatic
  fun populateOperatorConfigFromApi(
    operatorConfig: OperatorConfiguration,
    standardSyncOperation: StandardSyncOperation,
    standardWorkspace: StandardWorkspace,
  ) {
    standardSyncOperation.withOperatorType(operatorConfig.operatorType.convertTo<StandardSyncOperation.OperatorType>())
    if (operatorConfig.operatorType == OperatorType.WEBHOOK) {
      val webhook = requireNotNull(operatorConfig.webhook)
      // TODO(mfsiega-airbyte): check that the webhook config id references a real webhook config.
      val webhookConfigs =
        standardWorkspace.webhookOperationConfigs?.let { node: JsonNode ->
          Jsons.`object`(node, WebhookOperationConfigs::class.java)
        }
      var customDbtHost = Optional.empty<String>()
      if (webhookConfigs != null && webhookConfigs.webhookConfigs != null) {
        for (config in webhookConfigs.webhookConfigs) {
          if (DBT_CLOUD_WEBHOOK_TYPE == config.name) {
            customDbtHost = Optional.ofNullable(config.customDbtHost)
          }
        }
      }
      standardSyncOperation.withOperatorWebhook(webhookOperatorFromConfig(webhook, customDbtHost))
    }
  }

  @JvmStatic
  fun operationReadFromPersistedOperation(standardSyncOperation: StandardSyncOperation): OperationRead {
    // operator_type is a non-null column
    val operatorType = requireNotNull(standardSyncOperation.operatorType) { "Operation ${standardSyncOperation.operationId} has no operator type" }
    val webhook =
      if (operatorType == StandardSyncOperation.OperatorType.WEBHOOK) {
        webhookOperatorFromPersistence(requireNotNull(standardSyncOperation.operatorWebhook))
      } else {
        null
      }
    return OperationRead(
      workspaceId = standardSyncOperation.workspaceId,
      operationId = standardSyncOperation.operationId,
      name = standardSyncOperation.name,
      operatorConfiguration = OperatorConfiguration(operatorType = operatorType.convertTo<OperatorType>(), webhook = webhook),
    )
  }

  private fun webhookOperatorFromConfig(
    webhookConfig: OperatorWebhook,
    customDbtHost: Optional<String>,
  ): io.airbyte.config.OperatorWebhook {
    val operatorWebhook =
      io.airbyte.config
        .OperatorWebhook()
        .withWebhookConfigId(webhookConfig.webhookConfigId)
    // TODO(mfsiega-airbyte): remove this once the frontend is sending the new format.
    if (webhookConfig.webhookType == null) {
      return operatorWebhook
        .withExecutionUrl(webhookConfig.executionUrl)
        .withExecutionBody(webhookConfig.executionBody)
    }
    when (webhookConfig.webhookType!!) {
      OperatorWebhook.WebhookType.DBT_CLOUD -> {
        return operatorWebhook
          .withExecutionUrl(DbtCloudOperationConverter.getExecutionUrlFrom(webhookConfig.dbtCloud!!, customDbtHost))
          .withExecutionBody(DbtCloudOperationConverter.dbtCloudExecutionBody)
      }
    }
    throw IllegalArgumentException("Unsupported webhook operation type")
  }

  private fun webhookOperatorFromPersistence(persistedWebhook: io.airbyte.config.OperatorWebhook): OperatorWebhook {
    val dbtCloudOperator =
      DbtCloudOperationConverter.parseFrom(persistedWebhook) ?: throw IllegalArgumentException("Unexpected webhook operator config")
    return OperatorWebhook(
      webhookConfigId = persistedWebhook.webhookConfigId,
      webhookType = OperatorWebhook.WebhookType.DBT_CLOUD,
      dbtCloud = dbtCloudOperator,
      // TODO(mfsiega-airbyte): remove once frontend switches to new format.
      // Dual-write deprecated webhook format.
      executionUrl = persistedWebhook.executionUrl,
      executionBody = persistedWebhook.executionBody,
    )
  }

  object DbtCloudOperationConverter {
    // See https://docs.getdbt.com/dbt-cloud/api-v2 for documentation on dbt Cloud API endpoints.
    val dbtUrlPattern: Pattern = Pattern.compile("^https://(.*)/api/v2/accounts/(\\d+)/jobs/(\\d+)/run/$")
    private const val ACCOUNT_REGEX_GROUP = 2
    private const val JOB_REGEX_GROUP = 3

    // TODO(malik): we should find a way to actually persist the accound and job information instead of
    // re-extracting them
    @JvmStatic
    fun parseFrom(persistedWebhook: io.airbyte.config.OperatorWebhook): OperatorWebhookDbtCloud? {
      val dbtCloudUrlMatcher = dbtUrlPattern.matcher(persistedWebhook.executionUrl)
      if (!dbtCloudUrlMatcher.matches()) {
        return null
      }
      return OperatorWebhookDbtCloud(
        accountId = dbtCloudUrlMatcher.group(ACCOUNT_REGEX_GROUP).toLong(),
        jobId = dbtCloudUrlMatcher.group(JOB_REGEX_GROUP).toLong(),
      )
    }

    internal fun getExecutionUrlFrom(
      dbtCloudConfig: OperatorWebhookDbtCloud,
      customDbtHost: Optional<String>,
    ): String {
      var host = customDbtHost.orElse("cloud.getdbt.com")
      if (host.endsWith("/")) {
        host = host.substring(0, host.length - 1)
      }
      return String.format(
        "https://%s/api/v2/accounts/%d/jobs/%d/run/",
        host,
        dbtCloudConfig.accountId,
        dbtCloudConfig.jobId,
      )
    }

    val dbtCloudExecutionBody: String
      get() = "{\"cause\": \"airbyte\"}"
  }
}
