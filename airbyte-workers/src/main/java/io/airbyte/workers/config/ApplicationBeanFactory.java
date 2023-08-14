/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.AirbyteConfigValidator;
import io.airbyte.config.Configs.SecretPersistenceType;
import io.airbyte.config.Configs.TrackingStrategy;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.split_secrets.JsonSecretsProcessor;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.airbyte.micronaut.config.AirbyteConfigurationBeanFactory;
import io.airbyte.persistence.job.DefaultJobCreator;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.workers.internal.state_aggregator.StateAggregatorFactory;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Micronaut bean factory for general singletons.
 */
@Factory
@Slf4j
@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "MissingJavadocMethod"})
public class ApplicationBeanFactory {

  @Singleton
  public SecretPersistenceType secretPersistenceType(@Value("${airbyte.secret.persistence}") final String secretPersistence) {
    return AirbyteConfigurationBeanFactory.convertToEnum(secretPersistence, SecretPersistenceType::valueOf,
        SecretPersistenceType.TESTING_CONFIG_DB_TABLE);
  }

  @Singleton
  public TrackingStrategy trackingStrategy(@Value("${airbyte.tracking-strategy}") final String trackingStrategy) {
    return AirbyteConfigurationBeanFactory.convertToEnum(trackingStrategy, TrackingStrategy::valueOf, TrackingStrategy.LOGGING);
  }

  @Singleton
  @Named("workspaceRoot")
  public Path workspaceRoot(@Value("${airbyte.workspace.root}") final String workspaceRoot) {
    return Path.of(workspaceRoot);
  }

  @Singleton
  @Named("currentSecondsSupplier")
  public Supplier<Long> currentSecondsSupplier() {
    return () -> Instant.now().getEpochSecond();
  }

  @Singleton
  public DefaultJobCreator defaultJobCreator(final JobPersistence jobPersistence,
                                             final WorkerConfigsProvider workerConfigsProvider,
                                             final FeatureFlagClient featureFlagClient) {
    return new DefaultJobCreator(jobPersistence, workerConfigsProvider, featureFlagClient);
  }

  @Singleton
  public FeatureFlags featureFlags() {
    return new EnvVariableFeatureFlags();
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public JobNotifier jobNotifier(
                                 final ConfigRepository configRepository,
                                 final TrackingClient trackingClient,
                                 final WebUrlHelper webUrlHelper,
                                 final WorkspaceHelper workspaceHelper,
                                 final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    return new JobNotifier(
        webUrlHelper,
        configRepository,
        workspaceHelper,
        trackingClient,
        actorDefinitionVersionHelper);
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public JobTracker jobTracker(
                               final ConfigRepository configRepository,
                               final JobPersistence jobPersistence,
                               final TrackingClient trackingClient,
                               final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    return new JobTracker(configRepository, jobPersistence, trackingClient, actorDefinitionVersionHelper);
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public JsonSecretsProcessor jsonSecretsProcessor(final FeatureFlags featureFlags) {
    return JsonSecretsProcessor.builder()
        .copySecrets(false)
        .build();
  }

  @Singleton
  public AirbyteProtocolVersionRange airbyteProtocolVersionRange(
                                                                 @Value("${airbyte.protocol.min-version}") final String minVersion,
                                                                 @Value("${airbyte.protocol.max-version}") final String maxVersion) {
    return new AirbyteProtocolVersionRange(new Version(minVersion), new Version(maxVersion));
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public WebUrlHelper webUrlHelper(@Value("${airbyte.web-app.url}") final String webAppUrl) {
    return new WebUrlHelper(webAppUrl);
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public WorkspaceHelper workspaceHelper(
                                         final ConfigRepository configRepository,
                                         final JobPersistence jobPersistence) {
    return new WorkspaceHelper(
        configRepository,
        jobPersistence);
  }

  @Singleton
  public AirbyteConfigValidator airbyteConfigValidator() {
    return new AirbyteConfigValidator();
  }

  @Singleton
  public MetricClient metricClient() {
    // Initialize the metric client
    MetricClientFactory.initialize(MetricEmittingApps.WORKER);
    return MetricClientFactory.getMetricClient();
  }

  @Prototype
  @Named("syncPersistenceExecutorService")
  public ScheduledExecutorService syncPersistenceExecutorService() {
    return Executors.newSingleThreadScheduledExecutor();
  }

  @Singleton
  public StateAggregatorFactory stateAggregatorFactory(final FeatureFlags featureFlags) {
    return new StateAggregatorFactory(featureFlags);
  }

  @Singleton
  public LogClientSingleton logClientSingleton() {
    return LogClientSingleton.getInstance();
  }

}
