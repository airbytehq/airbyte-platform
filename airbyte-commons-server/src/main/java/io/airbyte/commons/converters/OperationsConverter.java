/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import static io.airbyte.api.model.generated.OperatorWebhook.WebhookTypeEnum.DBT_CLOUD;

import com.google.common.base.Preconditions;
import io.airbyte.api.model.generated.OperationRead;
import io.airbyte.api.model.generated.OperatorConfiguration;
import io.airbyte.api.model.generated.OperatorWebhookDbtCloud;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.OperatorWebhook;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardWorkspace;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Convert between API and internal versions of operations models.
 */
@SuppressWarnings("MissingSwitchDefault")
public class OperationsConverter {

  public static void populateOperatorConfigFromApi(final OperatorConfiguration operatorConfig,
                                                   final StandardSyncOperation standardSyncOperation,
                                                   final StandardWorkspace standardWorkspace) {
    standardSyncOperation.withOperatorType(Enums.convertTo(operatorConfig.getOperatorType(), OperatorType.class));
    switch (operatorConfig.getOperatorType()) {
      case WEBHOOK -> {
        Preconditions.checkArgument(operatorConfig.getWebhook() != null);
        // TODO(mfsiega-airbyte): check that the webhook config id references a real webhook config.
        final Optional<String> customDbtHost = Optional.ofNullable(standardWorkspace.getWebhookOperationConfigs())
            .flatMap(webHookOperationConfig -> {
              if (webHookOperationConfig.has("customDbtHost") && webHookOperationConfig.get("customDbtHost").isTextual()) {
                return Optional.of(webHookOperationConfig.get("customDbtHost").asText());
              }
              return Optional.empty();
            });

        standardSyncOperation.withOperatorWebhook(webhookOperatorFromConfig(operatorConfig.getWebhook(), customDbtHost));
      }
    }
  }

  public static OperationRead operationReadFromPersistedOperation(final StandardSyncOperation standardSyncOperation) {
    final OperatorConfiguration operatorConfiguration = new OperatorConfiguration()
        .operatorType(Enums.convertTo(standardSyncOperation.getOperatorType(), io.airbyte.api.model.generated.OperatorType.class));
    if (standardSyncOperation.getOperatorType() == null) {
      // TODO(mfsiega-airbyte): this case shouldn't happen, but the API today would tolerate it. After
      // verifying that it really can't happen, turn this into a precondition.
      return new OperationRead()
          .workspaceId(standardSyncOperation.getWorkspaceId())
          .operationId(standardSyncOperation.getOperationId())
          .name(standardSyncOperation.getName());
    }
    switch (standardSyncOperation.getOperatorType()) {
      case WEBHOOK -> {
        Preconditions.checkArgument(standardSyncOperation.getOperatorWebhook() != null);
        operatorConfiguration.webhook(webhookOperatorFromPersistence(standardSyncOperation.getOperatorWebhook()));
      }
    }
    return new OperationRead()
        .workspaceId(standardSyncOperation.getWorkspaceId())
        .operationId(standardSyncOperation.getOperationId())
        .name(standardSyncOperation.getName())
        .operatorConfiguration(operatorConfiguration);
  }

  private static OperatorWebhook webhookOperatorFromConfig(io.airbyte.api.model.generated.OperatorWebhook webhookConfig,
                                                           final Optional<String> customDbtHost) {
    final var operatorWebhook = new OperatorWebhook().withWebhookConfigId(webhookConfig.getWebhookConfigId());
    // TODO(mfsiega-airbyte): remove this once the frontend is sending the new format.
    if (webhookConfig.getWebhookType() == null) {
      return operatorWebhook
          .withExecutionUrl(webhookConfig.getExecutionUrl())
          .withExecutionBody(webhookConfig.getExecutionBody());
    }
    switch (webhookConfig.getWebhookType()) {
      case DBT_CLOUD -> {
        return operatorWebhook
            .withExecutionUrl(DbtCloudOperationConverter.getExecutionUrlFrom(webhookConfig.getDbtCloud(), customDbtHost))
            .withExecutionBody(DbtCloudOperationConverter.getDbtCloudExecutionBody());
      }
      // Future webhook operator types added here.
    }
    throw new IllegalArgumentException("Unsupported webhook operation type");
  }

  private static io.airbyte.api.model.generated.OperatorWebhook webhookOperatorFromPersistence(final OperatorWebhook persistedWebhook) {
    final io.airbyte.api.model.generated.OperatorWebhook webhookOperator = new io.airbyte.api.model.generated.OperatorWebhook()
        .webhookConfigId(persistedWebhook.getWebhookConfigId());
    OperatorWebhookDbtCloud dbtCloudOperator = DbtCloudOperationConverter.parseFrom(persistedWebhook);
    if (dbtCloudOperator != null) {
      webhookOperator.webhookType(DBT_CLOUD).dbtCloud(dbtCloudOperator);
      // TODO(mfsiega-airbyte): remove once frontend switches to new format.
      // Dual-write deprecated webhook format.
      webhookOperator.executionUrl(persistedWebhook.getExecutionUrl());
      webhookOperator.executionBody(persistedWebhook.getExecutionBody());
    } else {
      throw new IllegalArgumentException("Unexpected webhook operator config");
    }
    return webhookOperator;
  }

  public static class DbtCloudOperationConverter {

    // See https://docs.getdbt.com/dbt-cloud/api-v2 for documentation on dbt Cloud API endpoints.
    static final Pattern dbtUrlPattern = Pattern.compile("^https://cloud\\.getdbt\\.com/api/v2/accounts/(\\d+)/jobs/(\\d+)/run/$");
    private static final int ACCOUNT_REGEX_GROUP = 1;
    private static final int JOB_REGEX_GROUP = 2;

    private static OperatorWebhookDbtCloud parseFrom(OperatorWebhook persistedWebhook) {
      Matcher dbtCloudUrlMatcher = dbtUrlPattern.matcher(persistedWebhook.getExecutionUrl());
      final var dbtCloudConfig = new OperatorWebhookDbtCloud();
      if (dbtCloudUrlMatcher.matches()) {
        dbtCloudConfig.setAccountId(Integer.valueOf(dbtCloudUrlMatcher.group(ACCOUNT_REGEX_GROUP)));
        dbtCloudConfig.setJobId(Integer.valueOf(dbtCloudUrlMatcher.group(JOB_REGEX_GROUP)));
        return dbtCloudConfig;
      }
      return null;
    }

    private static String getExecutionUrlFrom(final OperatorWebhookDbtCloud dbtCloudConfig, final Optional<String> customDbtHost) {
      return String.format("https://%s/api/v2/accounts/%d/jobs/%d/run/", customDbtHost.orElse("cloud.getdbt.com"), dbtCloudConfig.getAccountId(),
          dbtCloudConfig.getJobId());
    }

    private static String getDbtCloudExecutionBody() {
      return "{\"cause\": \"airbyte\"}";
    }

  }

}
