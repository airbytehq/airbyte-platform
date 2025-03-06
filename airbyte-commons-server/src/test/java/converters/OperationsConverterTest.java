/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package converters;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.OperatorConfiguration;
import io.airbyte.api.model.generated.OperatorType;
import io.airbyte.api.model.generated.OperatorWebhook;
import io.airbyte.api.model.generated.OperatorWebhookDbtCloud;
import io.airbyte.commons.converters.OperationsConverter;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class OperationsConverterTest {

  @Test
  void testPopulateWebhookInfoWithCustomUrl() {
    final Long accountId = 834129412L;
    final Long jobId = 398431L;
    final OperatorConfiguration operatorConfig = new OperatorConfiguration()
        .operatorType(OperatorType.WEBHOOK)
        .webhook(new OperatorWebhook()
            .webhookType(OperatorWebhook.WebhookTypeEnum.DBT_CLOUD)
            .dbtCloud(new OperatorWebhookDbtCloud().accountId(accountId).jobId(jobId)));
    final StandardSyncOperation standardSyncOperation = new StandardSyncOperation();
    final JsonNode webHookOperationConfig = Jsons.deserialize("""
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
                                                              """);
    final StandardWorkspace standardWorkspace = new StandardWorkspace().withWebhookOperationConfigs(webHookOperationConfig);

    OperationsConverter.populateOperatorConfigFromApi(operatorConfig, standardSyncOperation, standardWorkspace);

    final String expectedUrl = "https://test-case-url.us1.dbt.com/api/v2/accounts/834129412/jobs/398431/run/";
    final String actualUrl = standardSyncOperation.getOperatorWebhook().getExecutionUrl();
    assert actualUrl.equals(expectedUrl) : String.format("Expected: %s, Actual: %s", expectedUrl, actualUrl);
  }

  @Test
  void testPopulateWebhookNoCustomUrl() {
    final Long accountId = 834129412L;
    final Long jobId = 398431L;
    final OperatorConfiguration operatorConfig = new OperatorConfiguration()
        .operatorType(OperatorType.WEBHOOK)
        .webhook(new OperatorWebhook()
            .webhookType(OperatorWebhook.WebhookTypeEnum.DBT_CLOUD)
            .dbtCloud(new OperatorWebhookDbtCloud().accountId(accountId).jobId(jobId)));
    final StandardSyncOperation standardSyncOperation = new StandardSyncOperation();
    final JsonNode webHookOperationConfig = Jsons.deserialize("""
                                                              {
                                                                "webhookConfigs": [
                                                                  {
                                                                    "id": "d1539c1e-0a9c-46f9-a4aa-718ca6fc3e08",
                                                                    "name": "dbt cloud",
                                                                    "authToken": "secret"
                                                                  }
                                                                ]
                                                              }
                                                              """);
    final StandardWorkspace standardWorkspace = new StandardWorkspace().withWebhookOperationConfigs(webHookOperationConfig);

    OperationsConverter.populateOperatorConfigFromApi(operatorConfig, standardSyncOperation, standardWorkspace);

    final String expectedUrl = "https://cloud.getdbt.com/api/v2/accounts/834129412/jobs/398431/run/";
    final String actualUrl = standardSyncOperation.getOperatorWebhook().getExecutionUrl();
    assert actualUrl.equals(expectedUrl) : String.format("Expected: %s, Actual: %s", expectedUrl, actualUrl);
  }

  @Test
  void testParseDbtUrl() {
    io.airbyte.config.OperatorWebhook operatorWebHook =
        new io.airbyte.config.OperatorWebhook().withExecutionUrl("https://cloud.getdbt.com/api/v2/accounts/834129412/jobs/398431/run/");
    OperatorWebhookDbtCloud result = OperationsConverter.DbtCloudOperationConverter.parseFrom(operatorWebHook);
    assert result.getAccountId().equals(834129412L);
    assert result.getJobId().equals(398431L);
  }

  @Test
  void testParseCustomDbtHostUrl() {
    io.airbyte.config.OperatorWebhook operatorWebHook =
        new io.airbyte.config.OperatorWebhook().withExecutionUrl("https://test-case-url.us1.dbt.com/api/v2/accounts/34128473/jobs/3434535/run/");
    OperatorWebhookDbtCloud result = OperationsConverter.DbtCloudOperationConverter.parseFrom(operatorWebHook);
    assert result.getAccountId().equals(34128473L);
    assert result.getJobId().equals(3434535L);
  }

}
