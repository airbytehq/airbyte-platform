/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package converters

import io.airbyte.api.server.generated.models.OperatorConfiguration
import io.airbyte.api.server.generated.models.OperatorType
import io.airbyte.api.server.generated.models.OperatorWebhook
import io.airbyte.api.server.generated.models.OperatorWebhookDbtCloud
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
      OperatorConfiguration(
        operatorType = OperatorType.WEBHOOK,
        webhook =
          OperatorWebhook(
            webhookType = OperatorWebhook.WebhookType.DBT_CLOUD,
            dbtCloud = OperatorWebhookDbtCloud(accountId = accountId, jobId = jobId),
          ),
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
    val actualUrl = standardSyncOperation.getOperatorWebhook().executionUrl
    assert(actualUrl == expectedUrl) { String.format("Expected: %s, Actual: %s", expectedUrl, actualUrl) }
  }

  @Test
  fun testPopulateWebhookNoCustomUrl() {
    val accountId = 834129412L
    val jobId = 398431L
    val operatorConfig =
      OperatorConfiguration(
        operatorType = OperatorType.WEBHOOK,
        webhook =
          OperatorWebhook(
            webhookType = OperatorWebhook.WebhookType.DBT_CLOUD,
            dbtCloud = OperatorWebhookDbtCloud(accountId = accountId, jobId = jobId),
          ),
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
    val actualUrl = standardSyncOperation.getOperatorWebhook().executionUrl
    assert(actualUrl == expectedUrl) { String.format("Expected: %s, Actual: %s", expectedUrl, actualUrl) }
  }

  @Test
  fun testParseDbtUrl() {
    val operatorWebHook =
      io.airbyte.config
        .OperatorWebhook()
        .withExecutionUrl("https://cloud.getdbt.com/api/v2/accounts/834129412/jobs/398431/run/")
    val result = parseFrom(operatorWebHook)
    assert(result!!.accountId == 834129412L)
    assert(result.jobId == 398431L)
  }

  @Test
  fun testParseCustomDbtHostUrl() {
    val operatorWebHook =
      io.airbyte.config
        .OperatorWebhook()
        .withExecutionUrl("https://test-case-url.us1.dbt.com/api/v2/accounts/34128473/jobs/3434535/run/")
    val result = parseFrom(operatorWebHook)
    assert(result!!.accountId == 34128473L)
    assert(result.jobId == 3434535L)
  }
}
