/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.google.common.base.Preconditions
import io.airbyte.api.model.generated.OperationRead
import io.airbyte.api.model.generated.OperatorConfiguration
import io.airbyte.api.model.generated.OperatorType
import io.airbyte.api.model.generated.OperatorWebhook
import io.airbyte.api.model.generated.OperatorWebhook.WebhookTypeEnum
import io.airbyte.api.model.generated.OperatorWebhookDbtCloud
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.WebhookOperationConfigs
import java.util.Objects
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
    standardSyncOperation.withOperatorType(operatorConfig.operatorType?.convertTo<StandardSyncOperation.OperatorType>())
    if (Objects.requireNonNull(operatorConfig.operatorType) == OperatorType.WEBHOOK) {
      Preconditions.checkArgument(operatorConfig.webhook != null)
      // TODO(mfsiega-airbyte): check that the webhook config id references a real webhook config.
      val webhookConfigs =
        standardWorkspace.webhookOperationConfigs?.let {
          Jsons.`object`(it, WebhookOperationConfigs::class.java)
        }
      var customDbtHost = Optional.empty<String>()
      if (webhookConfigs != null && webhookConfigs.webhookConfigs != null) {
        for (config in webhookConfigs.webhookConfigs) {
          if (DBT_CLOUD_WEBHOOK_TYPE == config.name) {
            customDbtHost = Optional.ofNullable(config.customDbtHost)
          }
        }
      }
      standardSyncOperation.withOperatorWebhook(webhookOperatorFromConfig(operatorConfig.webhook, customDbtHost))
    }
  }

  @JvmStatic
  fun operationReadFromPersistedOperation(standardSyncOperation: StandardSyncOperation): OperationRead {
    val operatorConfiguration =
      OperatorConfiguration()
        .operatorType(standardSyncOperation.operatorType?.convertTo<OperatorType>())
    if (standardSyncOperation.operatorType == null) {
      // TODO(mfsiega-airbyte): this case shouldn't happen, but the API today would tolerate it. After
      // verifying that it really can't happen, turn this into a precondition.
      return OperationRead()
        .workspaceId(standardSyncOperation.workspaceId)
        .operationId(standardSyncOperation.operationId)
        .name(standardSyncOperation.name)
    }
    if (Objects.requireNonNull<StandardSyncOperation.OperatorType>(standardSyncOperation.operatorType) ==
      StandardSyncOperation.OperatorType.WEBHOOK
    ) {
      Preconditions.checkArgument(standardSyncOperation.operatorWebhook != null)
      operatorConfiguration.webhook(webhookOperatorFromPersistence(standardSyncOperation.operatorWebhook))
    }
    return OperationRead()
      .workspaceId(standardSyncOperation.workspaceId)
      .operationId(standardSyncOperation.operationId)
      .name(standardSyncOperation.name)
      .operatorConfiguration(operatorConfiguration)
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
    when (webhookConfig.webhookType) {
      WebhookTypeEnum.DBT_CLOUD -> {
        return operatorWebhook
          .withExecutionUrl(DbtCloudOperationConverter.getExecutionUrlFrom(webhookConfig.dbtCloud, customDbtHost))
          .withExecutionBody(DbtCloudOperationConverter.dbtCloudExecutionBody)
      }
    }
    throw IllegalArgumentException("Unsupported webhook operation type")
  }

  private fun webhookOperatorFromPersistence(persistedWebhook: io.airbyte.config.OperatorWebhook): OperatorWebhook {
    val webhookOperator =
      OperatorWebhook()
        .webhookConfigId(persistedWebhook.webhookConfigId)
    val dbtCloudOperator = DbtCloudOperationConverter.parseFrom(persistedWebhook)
    if (dbtCloudOperator != null) {
      webhookOperator.webhookType(WebhookTypeEnum.DBT_CLOUD).dbtCloud(dbtCloudOperator)
      // TODO(mfsiega-airbyte): remove once frontend switches to new format.
      // Dual-write deprecated webhook format.
      webhookOperator.executionUrl(persistedWebhook.executionUrl)
      webhookOperator.executionBody(persistedWebhook.executionBody)
    } else {
      throw IllegalArgumentException("Unexpected webhook operator config")
    }
    return webhookOperator
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
      val dbtCloudConfig = OperatorWebhookDbtCloud()
      if (dbtCloudUrlMatcher.matches()) {
        dbtCloudConfig.accountId = dbtCloudUrlMatcher.group(ACCOUNT_REGEX_GROUP).toLong()
        dbtCloudConfig.jobId = dbtCloudUrlMatcher.group(JOB_REGEX_GROUP).toLong()
        return dbtCloudConfig
      }
      return null
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
