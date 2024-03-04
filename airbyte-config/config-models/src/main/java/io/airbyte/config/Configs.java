/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.storage.CloudStorageConfigs;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * This interface defines the general variables for configuring Airbyte.
 * <p>
 * Please update the configuring-airbyte.md document when modifying this file.
 * <p>
 * Please also add one of the following tags to the env var accordingly:
 * <p>
 * 1. 'Internal-use only' if a var is mainly for Airbyte-only configuration. e.g. tracking, test or
 * Cloud related etc.
 * <p>
 * 2. 'Alpha support' if a var does not have proper support and should be used with care.
 */

@SuppressWarnings("PMD.BooleanGetMethodName")
public interface Configs {

  // CORE
  // General

  /**
   * Distinguishes internal Airbyte deployments. Internal-use only.
   */
  String getAirbyteRole();

  /**
   * Defines the Airbyte deployment version.
   */
  AirbyteVersion getAirbyteVersion();

  String getAirbyteVersionOrWarning();

  /**
   * Distinguishes internal Airbyte deployments. Internal-use only.
   */
  DeploymentMode getDeploymentMode();

  /**
   * Defines if the deployment is Docker or Kubernetes. Airbyte behaves accordingly.
   */
  WorkerEnvironment getWorkerEnvironment();

  /**
   * Defines the configs directory. Applies only to Docker, and is present in Kubernetes for backward
   * compatibility.
   */
  Path getConfigRoot();

  /**
   * Defines the Airbyte workspace directory. Applies only to Docker, and is present in Kubernetes for
   * backward compatibility.
   */
  Path getWorkspaceRoot();

  // Docker Only

  /**
   * Defines the name of the Airbyte docker volume.
   */
  String getWorkspaceDockerMount();

  /**
   * Defines the name of the docker mount that is used for local file handling. On Docker, this allows
   * connector pods to interact with a volume for "local file" operations.
   */
  String getLocalDockerMount();

  /**
   * Defines the docker network jobs are launched on with the new scheduler.
   */
  String getDockerNetwork();

  Path getLocalRoot();

  // Secrets

  /**
   * Defines the Secret Persistence type. None by default. Set to GOOGLE_SECRET_MANAGER to use Google
   * Secret Manager. Set to TESTING_CONFIG_DB_TABLE to use the database as a test. Set to VAULT to use
   * Hashicorp Vault. Alpha support. Undefined behavior will result if this is turned on and then off.
   */
  SecretPersistenceType getSecretPersistenceType();

  // Database

  /**
   * Define the Jobs Database user.
   */
  String getDatabaseUser();

  /**
   * Define the Jobs Database password.
   */
  String getDatabasePassword();

  /**
   * Define the Jobs Database url in the form of
   * jdbc:postgresql://${DATABASE_HOST}:${DATABASE_PORT/${DATABASE_DB}. Do not include username or
   * password.
   */
  String getDatabaseUrl();

  /**
   * Define the number of retention days for the temporal history.
   */
  int getTemporalRetentionInDays();

  /**
   * Define the number of minutes a retry job will attempt to run before timing out.
   */
  int getJobInitRetryTimeoutMinutes();

  /**
   * Defines whether job creation uses connector-specific resource requirements when spawning jobs.
   * Works on both Docker and Kubernetes. Defaults to false for ease of use in OSS trials of Airbyte
   * but recommended for production deployments.
   */
  boolean connectorSpecificResourceDefaultsEnabled();

  /**
   * Get datadog or OTEL metric client for Airbyte to emit metrics. Allows empty value
   */
  String getMetricClient();

  /**
   * If choosing OTEL as the metric client, Airbyte will emit metrics and traces to this provided
   * endpoint.
   */
  String getOtelCollectorEndpoint();

  /**
   * If using a LaunchDarkly feature flag client, this API key will be used.
   *
   * @return LaunchDarkly API key as a string.
   */
  String getLaunchDarklyKey();

  /**
   * Get the type of feature flag client to use.
   *
   * @return feature flag client
   */
  String getFeatureFlagClient();

  /**
   * Defines a default map of environment variables to use for any launched job containers. The
   * expected format is a JSON encoded String -> String map. Make sure to escape properly. Defaults to
   * an empty map.
   */
  Map<String, String> getJobDefaultEnvMap();

  // Jobs - Kube only

  /**
   * Define one or more Job pod tolerations. Tolerations are separated by ';'. Each toleration
   * contains k=v pairs mentioning some/all of key, effect, operator and value and separated by `,`.
   */
  List<TolerationPOJO> getJobKubeTolerations();

  /**
   * Define one or more Job pod node selectors. Each kv-pair is separated by a `,`. Used for the sync
   * job and as fallback in case job specific (spec, check, discover) node selectors are not defined.
   */
  Map<String, String> getJobKubeNodeSelectors();

  /**
   * Define an isolated kube node selectors, so we can run risky images in it.
   */
  Map<String, String> getIsolatedJobKubeNodeSelectors();

  /**
   * Define if we want to run custom connector related jobs in a separate node pool.
   */
  boolean getUseCustomKubeNodeSelector();

  /**
   * Define node selectors for Spec job pods specifically. Each kv-pair is separated by a `,`.
   */
  Map<String, String> getSpecJobKubeNodeSelectors();

  /**
   * Define node selectors for Check job pods specifically. Each kv-pair is separated by a `,`.
   */
  Map<String, String> getCheckJobKubeNodeSelectors();

  /**
   * Define node selectors for Discover job pods specifically. Each kv-pair is separated by a `,`.
   */
  Map<String, String> getDiscoverJobKubeNodeSelectors();

  /**
   * Define one or more Job pod annotations. Each kv-pair is separated by a `,`. Used for the sync job
   * and as fallback in case job specific (spec, check, discover) annotations are not defined.
   */
  Map<String, String> getJobKubeAnnotations();

  /**
   * Define one or more Job pod labels. Each kv-pair is separated by a `,`. Used for the sync job and
   * as fallback in case job specific (spec, check, discover) annotations are not defined.
   */
  Map<String, String> getJobKubeLabels();

  /**
   * Define the Job pod connector image pull policy.
   */
  String getJobKubeMainContainerImagePullPolicy();

  /**
   * Define the Job pod connector sidecar image pull policy.
   */
  String getJobKubeSidecarContainerImagePullPolicy();

  /**
   * Define the Job pod connector image pull secret. Useful when hosting private images.
   */
  List<String> getJobKubeMainContainerImagePullSecrets();

  /**
   * Define the Memory request for the Sidecar.
   */
  String getSidecarMemoryRequest();

  /**
   * Define the Memory limit for the Sidecar.
   */
  String getSidecarKubeMemoryLimit();

  /**
   * Define the CPU request for the Sidecar.
   */
  String getSidecarKubeCpuRequest();

  /**
   * Define the CPU limit for the Sidecar.
   */
  String getSidecarKubeCpuLimit();

  /**
   * Define the CPU request for the SOCAT Sidecar.
   */
  String getJobKubeSocatImage();

  /**
   * Define the CPU limit for the SOCAT Sidecar.
   */
  String getSocatSidecarKubeCpuLimit();

  /**
   * Define the Job pod socat image.
   */
  String getSocatSidecarKubeCpuRequest();

  /**
   * Define the Job pod busybox image.
   */
  String getJobKubeBusyboxImage();

  /**
   * Define the Job pod curl image pull.
   */
  String getJobKubeCurlImage();

  /**
   * Define the Kubernetes namespace Job pods are created in.
   */
  String getJobKubeNamespace();

  // Logging/Monitoring/Tracking

  /**
   * Define either S3, Minio or GCS as a logging backend. Kubernetes only. Multiple variables are
   * involved here. Please see {@link CloudStorageConfigs} for more info.
   */
  LogConfigs getLogConfigs();

  /**
   * Defines the optional Google application credentials used for logging.
   */
  String getGoogleApplicationCredentials();

  /**
   * Define either S3, Minio or GCS as a state storage backend. Multiple variables are involved here.
   * Please see {@link CloudStorageConfigs} for more info.
   */
  CloudStorageConfigs getStateStorageCloudConfigs();

  /**
   * Determine if Datadog tracking events should be published. Mainly for Airbyte internal use.
   */
  boolean getPublishMetrics();

  /**
   * Set the Agent to publish Datadog metrics to. Only relevant if metrics should be published. Mainly
   * for Airbyte internal use.
   */
  String getDDAgentHost();

  /**
   * Set the port to publish Datadog metrics to. Only relevant if metrics should be published. Mainly
   * for Airbyte internal use.
   */
  String getDDDogStatsDPort();

  /**
   * Set constant tags to be attached to all metrics. Useful for distinguishing between environments.
   * Example: airbyte_instance:dev,k8s-cluster:aws-dev
   */
  List<String> getDDConstantTags();

  /**
   * Define whether to send job failure events to Sentry or log-only. Airbyte internal use.
   */
  JobErrorReportingStrategy getJobErrorReportingStrategy();

  /**
   * Determines the Sentry DSN that should be used when reporting connector job failures to Sentry.
   * Used with SENTRY error reporting strategy. Airbyte internal use.
   */
  String getJobErrorReportingSentryDSN();

  // APPLICATIONS
  // Worker

  /**
   * Connector Builder configs.
   */
  String getCdkPython();

  String getCdkEntrypoint();

  String getCustomerIoKey();

  /**
   * Job error reporting strategy.
   */
  enum JobErrorReportingStrategy {
    SENTRY,
    LOGGING
  }

  /**
   * Worker environment.
   */
  enum WorkerEnvironment {
    DOCKER,
    KUBERNETES
  }

  /**
   * Deployment type.
   */
  enum DeploymentMode {
    OSS,
    CLOUD
  }

  /**
   * Secret persistence type.
   */
  enum SecretPersistenceType {
    NONE,
    TESTING_CONFIG_DB_TABLE,
    GOOGLE_SECRET_MANAGER,
    VAULT,
    AWS_SECRET_MANAGER
  }

  /**
   * The configured Airbyte edition for the instance. By default, an Airbyte instance is configured as
   * Community edition. If configured as Pro edition, the instance will perform a license check and
   * activate additional features if valid.
   */
  enum AirbyteEdition {
    COMMUNITY,
    PRO
  }

}
