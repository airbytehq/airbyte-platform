/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import io.micronaut.context.annotation.ConfigurationProperties
import java.nio.file.Path
import java.time.Duration

/**
 * Where connector fat jars are published, how they are cached, and the check's limits. The jar for
 * `airbyte/source-postgres:3.8.10` is `io.airbyte.connectors:source-postgres:3.8.10` in [jarRepositoryUrl].
 */
@ConfigurationProperties("airbyte.direct-job-runner.check-connector")
data class CheckConnectorConfig(
  /** The Maven repository connector fat jars are published to. */
  val jarRepositoryUrl: String = "https://airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars",
  val jarGroupId: String = "io.airbyte.connectors",
  /** The local Maven repository downloaded jars are kept in. */
  val jarCacheDir: String = "/tmp/airbyte-connector-jars",
  /**
   * The longest a check may spend downloading a jar that isn't cached; a longer download fails the check. It is added to
   * [checkTimeout] for the workload's deadline.
   */
  val jarDownloadAllowance: Duration = Duration.ofMinutes(2),
  /** The JVM that runs the connector. Blank means the runner's own, which is Java 21 like the connector images. */
  val javaExecutable: String = "",
  val checkTimeout: Duration = Duration.ofSeconds(60),
  val maxHeap: String = "512m",
  /** CPUs each connector JVM sizes its GC and JIT threads for. Without it every JVM sizes them for the whole node. */
  val activeProcessorCount: Int = 2,
) {
  /**
   * The jar's Maven coordinates: `io.airbyte.connectors:source-postgres:3.8.10` for `airbyte/source-postgres:3.8.10`.
   * The version is the tag of the image's last path segment, so a registry port isn't taken for it.
   */
  fun jarCoordinates(image: String): String = jarCoordinates(connectorName(image), image.substringAfterLast('/').substringAfter(':'))

  /** The jar's Maven coordinates for [connector] (`source-postgres`) at [version]. */
  fun jarCoordinates(
    connector: String,
    version: String,
  ): String = "$jarGroupId:$connector:$version"

  val java: Path get() = if (javaExecutable.isEmpty()) Path.of(System.getProperty("java.home"), "bin", "java") else Path.of(javaExecutable)
}

/** The connector's name: `source-postgres` for `airbyte/source-postgres:3.8.10`. Taken after the last slash, so a registry port isn't used. */
fun connectorName(image: String): String = image.substringAfterLast('/').substringBefore(':')
