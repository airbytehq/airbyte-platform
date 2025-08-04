/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package converters

import io.airbyte.api.model.generated.OperatorConfiguration
import io.airbyte.api.model.generated.OperatorType
import io.airbyte.api.model.generated.OperatorWebhook
import io.airbyte.api.model.generated.OperatorWebhookDbtCloud
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.server.converters.OperationsConverter.DbtCloudOperationConverter.parseFrom
import io.airbyte.commons.server.converters.OperationsConverter.populateOperatorConfigFromApi
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import org.junit.jupiter.api.Test

internal class OperationsConverterTest {
  @Test
  fun testPopulateWebhookInfoWithCustomUrl() {
    val accountId = 834129412L
    val jobId = 398431L
    val operatorConfig =
      OperatorConfiguration()
        .operatorType(OperatorType.WEBHOOK)
        .webhook(
          OperatorWebhook()
            .webhookType(OperatorWebhook.WebhookTypeEnum.DBT_CLOUD)
            .dbtCloud(OperatorWebhookDbtCloud().accountId(accountId).jobId(jobId)),
        )
    val standardSyncOperation = StandardSyncOperation()
    val webHookOperationConfig =
      deserialize(
        """
        {
          "webhookConfigs": [
            {
              "id": "d1539c1e-0a9c-46f9-a4aa-718ca6fc3e08",
              "name": "dbt cloud",
              "authToken": "secret",
              "customDbtHost": "test-case-url.us1.dbt.com/"
            }
          ]
        }
        
        """.trimIndent(),
      )
    val standardWorkspace = StandardWorkspace().withWebhookOperationConfigs(webHookOperationConfig)

    populateOperatorConfigFromApi(operatorConfig, standardSyncOperation, standardWorkspace)

    val expectedUrl = "https://test-case-url.us1.dbt.com/api/v2/accounts/834129412/jobs/398431/run/"
    val actualUrl = standardSyncOperation.getOperatorWebhook().getExecutionUrl()
    assert(actualUrl == expectedUrl) { String.format("Expected: %s, Actual: %s", expectedUrl, actualUrl) }
  }

  @Test
  fun testPopulateWebhookNoCustomUrl() {
    val accountId = 834129412L
    val jobId = 398431L
    val operatorConfig =
      OperatorConfiguration()
        .operatorType(OperatorType.WEBHOOK)
        .webhook(
          OperatorWebhook()
            .webhookType(OperatorWebhook.WebhookTypeEnum.DBT_CLOUD)
            .dbtCloud(OperatorWebhookDbtCloud().accountId(accountId).jobId(jobId)),
        )
    val standardSyncOperation = StandardSyncOperation()
    val webHookOperationConfig =
      deserialize(
        """
        {
          "webhookConfigs": [
            {
              "id": "d1539c1e-0a9c-46f9-a4aa-718ca6fc3e08",
              "name": "dbt cloud",
              "authToken": "secret"
            }
          ]
        }
        
        """.trimIndent(),
      )
    val standardWorkspace = StandardWorkspace().withWebhookOperationConfigs(webHookOperationConfig)

    populateOperatorConfigFromApi(operatorConfig, standardSyncOperation, standardWorkspace)

    val expectedUrl = "https://cloud.getdbt.com/api/v2/accounts/834129412/jobs/398431/run/"
    val actualUrl = standardSyncOperation.getOperatorWebhook().getExecutionUrl()
    assert(actualUrl == expectedUrl) { String.format("Expected: %s, Actual: %s", expectedUrl, actualUrl) }
  }

  @Test
  fun testParseDbtUrl() {
    val operatorWebHook =
      io.airbyte.config
        .OperatorWebhook()
        .withExecutionUrl("https://cloud.getdbt.com/api/v2/accounts/834129412/jobs/398431/run/")
    val result = parseFrom(operatorWebHook)
    assert(result!!.getAccountId() == 834129412L)
    assert(result.getJobId() == 398431L)
  }

  @Test
  fun testParseCustomDbtHostUrl() {
    val operatorWebHook =
      io.airbyte.config
        .OperatorWebhook()
        .withExecutionUrl("https://test-case-url.us1.dbt.com/api/v2/accounts/34128473/jobs/3434535/run/")
    val result = parseFrom(operatorWebHook)
    assert(result!!.getAccountId() == 34128473L)
    assert(result.getJobId() == 3434535L)
  }
}
