/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.commons.version.AirbyteVersion
import java.nio.file.Path

/**
 * This interface defines the general variables for configuring Airbyte.
 *
 * Please update the configuring-airbyte.md document when modifying this file.
 *
 * Please also add one of the following tags to the env var accordingly:
 * 1. 'Internal-use only' if a var is mainly for Airbyte-only configuration.
 * 2. 'Alpha support' if a var does not have proper support and should be used with care.
 */
@Suppress("BooleanMethodNameMustStartWithQuestion")
interface Configs {
  // CORE
  // General

  /** Distinguishes internal Airbyte deployments. Internal-use only. */
  fun getAirbyteRole(): String?

  /** Defines the deployed edition of Airbyte. */
  fun getAirbyteEdition(): String?

  /** Defines the Airbyte deployment version. */
  fun getAirbyteVersion(): AirbyteVersion

  fun getAirbyteVersionOrWarning(): String

  /** Defines the Airbyte workspace directory. */
  fun getWorkspaceRoot(): Path

  // Database

  /** Define the Jobs Database user. */
  fun getDatabaseUser(): String

  /** Define the Jobs Database password. */
  fun getDatabasePassword(): String

  /** Define the Jobs Database URL. */
  fun getDatabaseUrl(): String

  // Jobs

  /** Define one or more Job pod tolerations. */
  fun getJobKubeTolerations(): List<TolerationPOJO>

  /** Define one or more Job pod node selectors. */
  fun getJobKubeNodeSelectors(): Map<String, String>?

  /** Define an isolated kube node selectors for risky images. */
  fun getIsolatedJobKubeNodeSelectors(): Map<String, String>

  /** Define if we want to use custom node selector. */
  fun getUseCustomKubeNodeSelector(): Boolean

  /** Define one or more Job pod annotations. */
  fun getJobKubeAnnotations(): Map<String, String>

  /** Define one or more Job pod labels. */
  fun getJobKubeLabels(): Map<String, String>

  /** Define the image pull policy for the job container. */
  fun getJobKubeMainContainerImagePullPolicy(): String

  /** Define the image pull secrets for the job container. */
  fun getJobKubeMainContainerImagePullSecrets(): List<String>

  /** Secret persistence type. */
  enum class SecretPersistenceType {
    NONE,
    TESTING_CONFIG_DB_TABLE,
    GOOGLE_SECRET_MANAGER,
    VAULT,
    AWS_SECRET_MANAGER,
  }

  /** Seed definitions provider type. */
  enum class SeedDefinitionsProviderType {
    LOCAL,
    REMOTE,
  }

  /** Configured Airbyte edition. */
  enum class AirbyteEdition {
    CLOUD,
    COMMUNITY,
    ENTERPRISE,
  }
}
