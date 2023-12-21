/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.orchestrator;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.Configs;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import jakarta.inject.Singleton;

@Singleton
public record CheckJobOrchestratorDataClass(
                                            WorkerConfigsProvider workerConfigsProvider,
                                            ProcessFactory processFactory,
                                            WorkloadIdGenerator workloadIdGenerator,
                                            Configs configs,
                                            WorkloadApi workloadApi,
                                            AirbyteApiClient airbyteApiClient,
                                            FeatureFlags featureFlags,
                                            GsonPksExtractor gsonPksExtractor,
                                            AirbyteMessageSerDeProvider serDeProvider,
                                            AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                            JobOutputDocStore jobOutputDocStore,
                                            FeatureFlagClient featureFlagClient) {

}
