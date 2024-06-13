/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.config.Configs;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.EnvConfigs;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.storage.StorageClient;
import io.airbyte.workers.sync.OrchestratorConstants;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Micronaut bean factory for container orchestrator configuration-related singletons.
 */
@Factory
public class ContainerOrchestratorConfigBeanFactory {

  private static final String METRIC_CLIENT_ENV_VAR = "METRIC_CLIENT";
  private static final String DD_AGENT_HOST_ENV_VAR = "DD_AGENT_HOST";
  private static final String DD_DOGSTATSD_PORT_ENV_VAR = "DD_DOGSTATSD_PORT";
  private static final String DD_ENV_ENV_VAR = "DD_ENV";
  private static final String DD_SERVICE_ENV_VAR = "DD_SERVICE";
  private static final String DD_VERSION_ENV_VAR = "DD_VERSION";
  private static final String JAVA_OPTS_ENV_VAR = "JAVA_OPTS";
  private static final String PUBLISH_METRICS_ENV_VAR = "PUBLISH_METRICS";
  private static final String CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR = "CONTROL_PLANE_AUTH_ENDPOINT";
  private static final String DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR = "DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH";
  private static final String DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR = "DATA_PLANE_SERVICE_ACCOUNT_EMAIL";
  private static final String AIRBYTE_API_AUTH_HEADER_NAME_ENV_VAR = "AIRBYTE_API_AUTH_HEADER_NAME";
  private static final String AIRBYTE_API_AUTH_HEADER_VALUE_ENV_VAR = "AIRBYTE_API_AUTH_HEADER_VALUE";
  private static final String INTERNAL_API_HOST_ENV_VAR = "INTERNAL_API_HOST";
  private static final String ACCEPTANCE_TEST_ENABLED_VAR = "ACCEPTANCE_TEST_ENABLED";
  private static final String DD_INTEGRATION_ENV_VAR_FORMAT = "DD_INTEGRATION_%s_ENABLED";

  @SuppressWarnings("LineLength")
  @Singleton
  @Requires(property = "airbyte.container.orchestrator.enabled",
            value = "true")
  @Named("containerOrchestratorConfig")
  public ContainerOrchestratorConfig kubernetesContainerOrchestratorConfig(
                                                                           @Named("stateDocumentStore") final StorageClient stateStorageClient,
                                                                           @Named("outputDocumentStore") final StorageClient outputDocumentStoreClient,
                                                                           @Value("${airbyte.version}") final String airbyteVersion,
                                                                           @Value("${airbyte.container.orchestrator.image}") final String containerOrchestratorImage,
                                                                           @Value("${airbyte.worker.job.kube.main.container.image-pull-policy}") final String containerOrchestratorImagePullPolicy,
                                                                           @Value("${airbyte.container.orchestrator.secret-mount-path}") final String containerOrchestratorSecretMountPath,
                                                                           @Value("${airbyte.container.orchestrator.secret-name}") final String containerOrchestratorSecretName,
                                                                           @Value("${google.application.credentials}") final String googleApplicationCredentials,
                                                                           @Value("${airbyte.worker.job.kube.namespace}") final String namespace,
                                                                           @Value("${airbyte.metric.client}") final String metricClient,
                                                                           @Value("${datadog.agent.host}") final String dataDogAgentHost,
                                                                           @Value("${datadog.agent.port}") final String dataDogStatsdPort,
                                                                           @Value("${airbyte.metric.should-publish}") final String shouldPublishMetrics,
                                                                           final FeatureFlags featureFlags,
                                                                           @Value("${airbyte.container.orchestrator.java-opts}") final String containerOrchestratorJavaOpts,
                                                                           final WorkerEnvironment workerEnvironment,
                                                                           /*
                                                                            * Reference the environment variable, instead of the resolved property, so
                                                                            * that the entry in the orchestrator's application.yml is consistent with
                                                                            * all other services that use the Airbyte API client.
                                                                            */
                                                                           @Value("${INTERNAL_API_HOST}") final String containerOrchestratorInternalApiHost,
                                                                           @Value("${airbyte.internal-api.auth-header.name}") final String containerOrchestratorInternalApiAuthHeaderName,
                                                                           @Value("${airbyte.internal-api.auth-header.value}") final String containerOrchestratorInternalApiAuthHeaderValue,
                                                                           @Value("${airbyte.control.plane.auth-endpoint}") final String controlPlaneAuthEndpoint,
                                                                           @Value("${airbyte.data.plane.service-account.email}") final String dataPlaneServiceAccountEmail,
                                                                           @Value("${airbyte.data.plane.service-account.credentials-path}") final String dataPlaneServiceAccountCredentialsPath,
                                                                           @Value("${airbyte.container.orchestrator.data-plane-creds.secret-mount-path}") final String containerOrchestratorDataPlaneCredsSecretMountPath,
                                                                           @Value("${airbyte.container.orchestrator.data-plane-creds.secret-name}") final String containerOrchestratorDataPlaneCredsSecretName,
                                                                           @Value("${airbyte.acceptance.test.enabled}") final boolean isInTestMode,
                                                                           @Value("${datadog.orchestrator.disabled.integrations}") final String disabledIntegrations,
                                                                           @Value("${airbyte.worker.job.kube.serviceAccount}") final String serviceAccount,
                                                                           final MetricClient metricClientInstance) {
    final var kubernetesClient = new DefaultKubernetesClient();

    final JobOutputDocStore jobOutputDocStore = new JobOutputDocStore(outputDocumentStoreClient, metricClientInstance);

    // Build the map of additional environment variables to be passed to the container orchestrator
    final Map<String, String> environmentVariables = new HashMap<>();
    environmentVariables.put(METRIC_CLIENT_ENV_VAR, metricClient);
    environmentVariables.put(DD_AGENT_HOST_ENV_VAR, dataDogAgentHost);
    environmentVariables.put(DD_SERVICE_ENV_VAR, "airbyte-container-orchestrator");
    environmentVariables.put(DD_DOGSTATSD_PORT_ENV_VAR, dataDogStatsdPort);
    environmentVariables.put(PUBLISH_METRICS_ENV_VAR, shouldPublishMetrics);
    environmentVariables.put(EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA, Boolean.toString(featureFlags.autoDetectSchema()));
    environmentVariables.put(EnvVariableFeatureFlags.APPLY_FIELD_SELECTION, Boolean.toString(featureFlags.applyFieldSelection()));
    environmentVariables.put(EnvVariableFeatureFlags.FIELD_SELECTION_WORKSPACES, featureFlags.fieldSelectionWorkspaces());
    environmentVariables.put(JAVA_OPTS_ENV_VAR, containerOrchestratorJavaOpts);
    environmentVariables.put(CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR, controlPlaneAuthEndpoint);
    environmentVariables.put(DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR, dataPlaneServiceAccountCredentialsPath);
    environmentVariables.put(DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR, dataPlaneServiceAccountEmail);

    // Disable DD agent integrations based on the configuration
    if (StringUtils.isNotEmpty(disabledIntegrations)) {
      Arrays.asList(disabledIntegrations.split(","))
          .forEach(e -> environmentVariables.put(String.format(DD_INTEGRATION_ENV_VAR_FORMAT, e.trim()), Boolean.FALSE.toString()));
    }

    final Configs configs = new EnvConfigs();
    environmentVariables.put(EnvVar.FEATURE_FLAG_CLIENT.name(), EnvVar.FEATURE_FLAG_CLIENT.fetch(""));
    environmentVariables.put(EnvVar.LAUNCHDARKLY_KEY.name(), EnvVar.LAUNCHDARKLY_KEY.fetch(""));
    environmentVariables.put(EnvVar.OTEL_COLLECTOR_ENDPOINT.name(), EnvVar.OTEL_COLLECTOR_ENDPOINT.fetch(""));
    environmentVariables.put(EnvVar.SOCAT_KUBE_CPU_LIMIT.name(), configs.getSocatSidecarKubeCpuLimit());
    environmentVariables.put(EnvVar.SOCAT_KUBE_CPU_REQUEST.name(), configs.getSocatSidecarKubeCpuRequest());

    if (System.getenv(DD_ENV_ENV_VAR) != null) {
      environmentVariables.put(DD_ENV_ENV_VAR, System.getenv(DD_ENV_ENV_VAR));
    }

    if (System.getenv(DD_VERSION_ENV_VAR) != null) {
      environmentVariables.put(DD_VERSION_ENV_VAR, System.getenv(DD_VERSION_ENV_VAR));
    }

    // Environment variables for ApiClientBeanFactory
    environmentVariables.put(CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR, controlPlaneAuthEndpoint);
    environmentVariables.put(DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR, dataPlaneServiceAccountCredentialsPath);
    environmentVariables.put(DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR, dataPlaneServiceAccountEmail);
    environmentVariables.put(AIRBYTE_API_AUTH_HEADER_NAME_ENV_VAR, containerOrchestratorInternalApiAuthHeaderName);
    environmentVariables.put(AIRBYTE_API_AUTH_HEADER_VALUE_ENV_VAR, containerOrchestratorInternalApiAuthHeaderValue);
    environmentVariables.put(INTERNAL_API_HOST_ENV_VAR, containerOrchestratorInternalApiHost);

    if (System.getenv(Environment.ENVIRONMENTS_ENV) != null) {
      environmentVariables.put(Environment.ENVIRONMENTS_ENV, System.getenv(Environment.ENVIRONMENTS_ENV));
    }

    environmentVariables.put(ACCEPTANCE_TEST_ENABLED_VAR, Boolean.toString(isInTestMode));

    // copy over all local values
    environmentVariables.putAll(System.getenv().entrySet().stream().filter(e -> OrchestratorConstants.ENV_VARS_TO_TRANSFER.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    return new ContainerOrchestratorConfig(
        namespace,
        stateStorageClient,
        environmentVariables,
        kubernetesClient,
        containerOrchestratorSecretName,
        containerOrchestratorSecretMountPath,
        containerOrchestratorDataPlaneCredsSecretName,
        containerOrchestratorDataPlaneCredsSecretMountPath,
        StringUtils.isNotEmpty(containerOrchestratorImage) ? containerOrchestratorImage : "airbyte/container-orchestrator:" + airbyteVersion,
        containerOrchestratorImagePullPolicy,
        googleApplicationCredentials,
        workerEnvironment,
        serviceAccount,
        jobOutputDocStore);
  }

}
