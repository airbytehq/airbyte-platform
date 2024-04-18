/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.config.NormalizationInput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import jakarta.annotation.Nullable;
import java.util.UUID;

/**
 * Normalization activity temporal interface.
 */
@ActivityInterface
public interface NormalizationActivity {

  @ActivityMethod
  NormalizationSummary normalize(JobRunConfig jobRunConfig,
                                 IntegrationLauncherConfig destinationLauncherConfig,
                                 NormalizationInput input);

  @ActivityMethod
  NormalizationInput generateNormalizationInputWithMinimumPayloadWithConnectionId(final JsonNode destinationConfiguration,
                                                                                  @Deprecated @Nullable final ConfiguredAirbyteCatalog airbyteCatalog,
                                                                                  final UUID workspaceId,
                                                                                  final UUID connectionId,
                                                                                  final UUID organizationId);

}
