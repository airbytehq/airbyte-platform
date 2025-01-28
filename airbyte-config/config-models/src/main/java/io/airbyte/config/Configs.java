/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import io.airbyte.commons.version.AirbyteVersion;
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
   * Defines the Airbyte workspace directory. Applies only to Docker, and is present in Kubernetes for
   * backward compatibility.
   */
  Path getWorkspaceRoot();

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

  // Jobs

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
   * Define the Job pod connector image pull secret. Useful when hosting private images.
   */
  List<String> getJobKubeMainContainerImagePullSecrets();

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
   * Seed definitions provider type.
   */
  enum SeedDefinitionsProviderType {
    LOCAL,
    REMOTE
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
