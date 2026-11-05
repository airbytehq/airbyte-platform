/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import io.micronaut.context.annotation.ConfigurationProperties
import java.nio.file.Path
import java.time.Duration

/** The connector distribution baked into this image whose checks the runner runs as a local process, and its limits. */
@ConfigurationProperties("airbyte.direct-job-runner.check-connector")
data class CheckConnectorConfig(
  /** The image the distribution came from, set by the Dockerfile. Blank means the runner takes no checks. */
  val image: String = "",
  /** Where the distribution is installed. Defaults to `/connectors/<name>`. */
  val installDir: String = "",
  val checkTimeout: Duration = Duration.ofSeconds(60),
  val maxHeap: String = "512m",
  /** CPUs each connector JVM sizes its GC and JIT threads for. Without it every JVM sizes them for the whole node. */
  val activeProcessorCount: Int = 2,
) {
  /** The connector's name: `source-postgres` for `airbyte/source-postgres:3.8.10`. */
  val name: String get() = image.substringAfterLast('/').substringBefore(':')

  /** The Gradle application launcher script, `bin/<name>`. */
  val launcher: Path get() = Path.of(installDir.ifEmpty { "/connectors/$name" }, "bin", name)

  /** The env var that launcher reads extra JVM options from: `SOURCE_POSTGRES_OPTS` for `source-postgres`. */
  val jvmOptsEnvVar: String get() = name.uppercase().replace('-', '_') + "_OPTS"
}
