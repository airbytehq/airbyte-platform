/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import io.micronaut.context.annotation.ConfigurationProperties
import java.time.Duration

private const val DEFAULT_REGISTRY_BASE_URL = "https://connectors.airbyte.com/files/"

/** Checks for Airbyte's manifest-only connectors, which the runner sends to its data plane's manifest server. */
@ConfigurationProperties("airbyte.direct-job-runner.manifest-server-check")
data class ManifestServerCheckConfig(
  /**
   * The CDK version the manifest server runs, such as `7.45.0`: the chart sets it from the server's image tag. Blank
   * means the runner sends no checks to the manifest server.
   */
  val serverCdkVersion: String = "",
  /** Where the connector registry publishes each connector version's metadata and manifest. */
  val registryBaseUrl: String = "",
  /**
   * Deciding whether to take a check runs inside the launcher's handoff call, which gives up after 10s and retries. A
   * version's first registry lookup (both of its reads) and one health call, each bounded as a whole, leave about half
   * of that for reading the workload and answering.
   */
  val registryTimeout: Duration = Duration.ofSeconds(3),
  val healthTimeout: Duration = Duration.ofSeconds(2),
  /** How long a health answer, good or bad, is reused, so a hung server costs one [healthTimeout], not one per check. */
  val healthCacheTtl: Duration = Duration.ofSeconds(5),
  /**
   * Connector repositories (`airbyte/source-stripe`) that ship custom Python components but whose check doesn't use
   * them: the CDK builds only the check streams, so the manifest server runs their check without the components.
   * Each entry must be verified against the manifest server first; custom code can also run outside the check streams,
   * such as in the spec's config normalization rules.
   */
  val componentsOptionalConnectors: List<String> = emptyList(),
) {
  /** The registry base URL; connector paths are appended to it, so an override must end in `/` like the default. */
  val registryBase: String get() = registryBaseUrl.ifBlank { DEFAULT_REGISTRY_BASE_URL }
}
