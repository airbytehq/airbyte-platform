/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.server.handlers.helpers.BuilderProjectUpdater;
import io.airbyte.commons.server.handlers.helpers.CompositeBuilderProjectUpdater;
import io.airbyte.commons.server.handlers.helpers.ConfigRepositoryBuilderProjectUpdater;
import io.airbyte.commons.server.handlers.helpers.LocalFileSystemBuilderProjectUpdater;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.server.scheduler.TemporalEventRunner;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.StreamRefreshesRepository;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.airbyte.persistence.job.DefaultJobCreator;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.factory.DefaultSyncJobFactory;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.factory.SyncJobFactory;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Bean factory for the airbyte server micronaut app.
 */
@Factory
public class ApplicationBeanFactory {

  @SuppressWarnings("AbbreviationAsWordInName")
  @Singleton
  @Named("uuidGenerator")
  public Supplier<UUID> randomUUIDSupplier() {
    return UUID::randomUUID;
  }

  @Singleton
  public EventRunner eventRunner(final TemporalClient temporalClient) {
    return new TemporalEventRunner(temporalClient);
  }

  @Singleton
  public JobTracker jobTracker(
                               final ConfigRepository configRepository,
                               final JobPersistence jobPersistence,
                               final TrackingClient trackingClient,
                               final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    return new JobTracker(configRepository, jobPersistence, trackingClient, actorDefinitionVersionHelper);
  }

  @Singleton
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
  public DefaultJobCreator defaultJobCreator(final JobPersistence jobPersistence,
                                             final WorkerConfigsProvider workerConfigsProvider,
                                             final FeatureFlagClient featureFlagClient,
                                             final StreamRefreshesRepository streamRefreshesRepository,
                                             @Value("${airbyte.worker.kube-job-config-variant-override}") final String variantOverride) {
    return new DefaultJobCreator(jobPersistence, workerConfigsProvider, featureFlagClient, streamRefreshesRepository, variantOverride);
  }

  @SuppressWarnings("ParameterName")
  @Singleton
  public SyncJobFactory jobFactory(
                                   final ConfigRepository configRepository,
                                   final JobPersistence jobPersistence,
                                   @Property(name = "airbyte.connector.specific-resource-defaults-enabled",
                                             defaultValue = "false") final boolean connectorSpecificResourceDefaultsEnabled,
                                   final DefaultJobCreator jobCreator,
                                   final OAuthConfigSupplier oAuthConfigSupplier,
                                   final ConfigInjector configInjector,
                                   final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    return new DefaultSyncJobFactory(
        connectorSpecificResourceDefaultsEnabled,
        jobCreator,
        configRepository,
        oAuthConfigSupplier,
        configInjector,
        new WorkspaceHelper(configRepository, jobPersistence),
        actorDefinitionVersionHelper);
  }

  @Singleton
  public WebUrlHelper webUrlHelper(@Value("${airbyte.web-app.url}") final String webAppUrl) {
    return new WebUrlHelper(webAppUrl);
  }

  @Singleton
  public MetricClient metricClient() {
    MetricClientFactory.initialize(MetricEmittingApps.SERVER);
    return MetricClientFactory.getMetricClient();
  }

  @Singleton
  @Named("workspaceRoot")
  public Path workspaceRoot(@Value("${airbyte.workspace.root}") final String workspaceRoot) {
    return Path.of(workspaceRoot);
  }

  @Singleton
  @Named("airbyteSupportEmailDomains")
  public Set<String> airbyteSupportEmailDomains(
                                                @Value("${airbyte.deployment-mode}") final String deployMode,
                                                @Value("${airbyte.support-email-domains.oss}") final String ossSupportEmailDomains,
                                                @Value("${airbyte.support-email-domains.cloud}") final String cloudSupportEmailDomains) {
    final String supportEmailDomains = Objects.equals(deployMode, DeploymentMode.OSS.name()) ? ossSupportEmailDomains : cloudSupportEmailDomains;
    if (supportEmailDomains.isEmpty()) {
      return Set.of();
    }
    return Arrays.stream(supportEmailDomains.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }

  @Singleton
  public JsonSecretsProcessor jsonSecretsProcessor() {
    return new JsonSecretsProcessor(false);
  }

  @Singleton
  @Named("jsonSecretsProcessorWithCopy")
  public JsonSecretsProcessor jsonSecretsProcessorWithCopy() {
    return new JsonSecretsProcessor(true);
  }

  @Singleton
  public JsonSchemaValidator jsonSchemaValidator() {
    return new JsonSchemaValidator();
  }

  @Singleton
  public AirbyteProtocolVersionRange airbyteProtocolVersionRange(
                                                                 @Value("${airbyte.protocol.min-version}") final String minVersion,
                                                                 @Value("${airbyte.protocol.max-version}") final String maxVersion) {
    return new AirbyteProtocolVersionRange(new Version(minVersion), new Version(maxVersion));
  }

  @Singleton
  @Named("oauthHttpClient")
  public HttpClient httpClient() {
    return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  }

  @Singleton
  public BuilderProjectUpdater builderProjectUpdater(final ConnectorBuilderService connectorBuilderService) {
    final var pathToConnectors = io.airbyte.commons.envvar.EnvVar.PATH_TO_CONNECTORS.fetch();
    final ConfigRepositoryBuilderProjectUpdater configRepositoryProjectUpdater = new ConfigRepositoryBuilderProjectUpdater(connectorBuilderService);
    if (pathToConnectors == null || pathToConnectors.isEmpty()) {
      return configRepositoryProjectUpdater;
    } else {
      return new CompositeBuilderProjectUpdater(List.of(configRepositoryProjectUpdater, new LocalFileSystemBuilderProjectUpdater()));
    }
  }

  @Singleton
  @Requires(env = Environment.KUBERNETES)
  public Optional<KubernetesClient> kubernetesClient() {
    try {
      return Optional.of(new KubernetesClientBuilder().build());
    } catch (final Exception e) {
      return Optional.empty();
    }
  }

}
