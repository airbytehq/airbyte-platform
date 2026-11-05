/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs
import io.airbyte.directjobrunner.config.CheckConnectorConfig
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.time.Duration

private const val SUCCEEDED = """{"type":"CONNECTION_STATUS","connectionStatus":{"status":"SUCCEEDED"}}"""

/** Each test replaces the connector launcher with a shell script, so no JVM or database is involved. */
class DirectCheckRunnerTest {
  @TempDir
  lateinit var installDir: Path

  @TempDir
  lateinit var captureDir: Path

  private val connectionConfiguration = Jsons.jsonNode(mapOf("host" to "db.example.com", "password" to "hunter2"))

  private fun check(
    launcherBody: String,
    checkTimeout: Duration = Duration.ofSeconds(10),
    edition: Configs.AirbyteEdition = Configs.AirbyteEdition.COMMUNITY,
  ): SourceCheckResult {
    val launcher = installDir.resolve("bin/source-postgres")
    Files.createDirectories(launcher.parent)
    Files.writeString(launcher, "#!/bin/bash\n$launcherBody\n")
    Files.setPosixFilePermissions(launcher, PosixFilePermissions.fromString("rwx------"))

    val config = CheckConnectorConfig(image = "airbyte/source-postgres:3.8.10", installDir = installDir.toString(), checkTimeout = checkTimeout)
    return DirectCheckRunner(config, edition).check(connectionConfiguration, "workload-1")
  }

  @Test
  fun `a reported success is returned and log or non-protocol lines are skipped`() {
    val result =
      check(
        """
        echo '{"type":"LOG","log":{"level":"INFO","message":"starting"}}'
        echo 'plain text from the JVM'
        echo '$SUCCEEDED'
        """.trimIndent(),
      )

    assertEquals(AirbyteConnectionStatus.Status.SUCCEEDED, (result as SourceCheckResult.Reported).status.status)
  }

  @Test
  fun `a reported failure is a normal result even when the connector exits non-zero`() {
    val result =
      check(
        """
        echo '{"type":"CONNECTION_STATUS","connectionStatus":{"status":"FAILED","message":"password authentication failed"}}'
        exit 1
        """.trimIndent(),
      )

    val status = (result as SourceCheckResult.Reported).status
    assertEquals(AirbyteConnectionStatus.Status.FAILED, status.status)
    assertEquals("password authentication failed", status.message)
  }

  @Test
  fun `an error trace without a status is a connector error`() {
    val result =
      check(
        """
        echo '{"type":"TRACE","trace":{"type":"ERROR","emitted_at":0,"error":{"message":"Invalid configuration","failure_type":"config_error"}}}'
        exit 1
        """.trimIndent(),
      )

    assertEquals("Invalid configuration", (result as SourceCheckResult.ConnectorError).error.message)
  }

  @Test
  fun `a non-zero exit without a status or error trace is a crash`() {
    assertEquals(SourceCheckResult.Crashed(3), check("exit 3"))
  }

  @Test
  fun `a zero exit without a status or error trace is a crash`() {
    assertEquals(SourceCheckResult.Crashed(0), check("exit 0"))
  }

  @Test
  fun `a message the protocol model cannot parse does not stop the reader`() {
    val result =
      check(
        """
        echo '{"type":"NOT_A_MESSAGE_TYPE"}'
        echo '$SUCCEEDED'
        """.trimIndent(),
      )

    assertInstanceOf(SourceCheckResult.Reported::class.java, result)
  }

  @Test
  fun `a connector that outlives the timeout is killed`() {
    val started = System.nanoTime()

    val result = check("exec sleep 30", checkTimeout = Duration.ofMillis(500))

    assertEquals(SourceCheckResult.TimedOut, result)
    assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofSeconds(10))
  }

  @Test
  fun `a status written just before exit is not lost`() {
    val result = check("for i in $(seq 1 5000); do echo \"line ${'$'}i\"; done; echo '$SUCCEEDED'")

    assertInstanceOf(SourceCheckResult.Reported::class.java, result)
  }

  @Test
  fun `the config is passed over stdin, not in a file`() {
    val capturedConfig = captureDir.resolve("config")

    val result =
      check(
        """
        [ "$1" = "--check" ] && [ "$2" = "--config" ] && [ "$3" = "/dev/stdin" ] || exit 9
        cat "$3" > $capturedConfig
        echo '$SUCCEEDED'
        """.trimIndent(),
      )

    assertInstanceOf(SourceCheckResult.Reported::class.java, result)
    assertEquals(connectionConfiguration, Jsons.deserialize(Files.readString(capturedConfig)))
  }

  @Test
  fun `a connector that exits without reading the config still reports its result`() {
    assertEquals(SourceCheckResult.Crashed(4), check("exec 0<&-; exit 4"))
  }

  @Test
  fun `the connector gets the JVM options and none of the runner's environment`() {
    val capturedEnv = captureDir.resolve("env")

    check("env > $capturedEnv; echo '$SUCCEEDED'")

    val env =
      Files
        .readAllLines(capturedEnv)
        .filter { it.contains('=') }
        .associate { it.substringBefore('=') to it.substringAfter('=') }
    assertEquals("-Xms64m -Xmx512m", env["SOURCE_POSTGRES_OPTS"])
    assertTrue(env.getValue("JAVA_OPTS").contains("--add-opens=java.base/java.lang=ALL-UNNAMED"))
    assertFalse(env.containsKey("HOME"))
    assertFalse(env.containsKey("USER"))
  }

  @Test
  fun `the connector gets the deployment mode and ids a check pod's connector gets`() {
    val capturedEnv = captureDir.resolve("env")

    fun envFor(edition: Configs.AirbyteEdition): Map<String, String> {
      check("env > $capturedEnv; echo '$SUCCEEDED'", edition = edition)
      return Files
        .readAllLines(capturedEnv)
        .filter { it.contains('=') }
        .associate { it.substringBefore('=') to it.substringAfter('=') }
    }

    val cloud = envFor(Configs.AirbyteEdition.CLOUD)
    assertEquals("CLOUD", cloud["AIRBYTE_EDITION"])
    assertEquals("CLOUD", cloud["DEPLOYMENT_MODE"])
    assertEquals("check", cloud["OPERATION_TYPE"])
    assertEquals("workload-1", cloud["WORKLOAD_ID"])

    val community = envFor(Configs.AirbyteEdition.COMMUNITY)
    assertEquals("COMMUNITY", community["AIRBYTE_EDITION"])
    assertEquals("OSS", community["DEPLOYMENT_MODE"])
  }

  @Test
  fun `a connector that logs a lot before reading a large config does not deadlock`() {
    // 200 KiB of stderr before reading stdin, and a config larger than a pipe buffer.
    val bigConfig = Jsons.jsonNode(mapOf("host" to "db.example.com", "ca_certificate" to "x".repeat(200_000)))
    val launcher = installDir.resolve("bin/source-postgres")
    Files.createDirectories(launcher.parent)
    Files.writeString(launcher, "#!/bin/bash\nhead -c 204800 /dev/zero | tr '\\0' 'a' >&2\ncat > /dev/null\necho '$SUCCEEDED'\n")
    Files.setPosixFilePermissions(launcher, PosixFilePermissions.fromString("rwx------"))
    val config =
      CheckConnectorConfig(image = "airbyte/source-postgres:3.8.10", installDir = installDir.toString(), checkTimeout = Duration.ofSeconds(10))

    val start = System.nanoTime()
    val result = DirectCheckRunner(config, Configs.AirbyteEdition.COMMUNITY).check(bigConfig, "workload-1")

    assertInstanceOf(SourceCheckResult.Reported::class.java, result)
    assertTrue(Duration.ofNanos(System.nanoTime() - start) < Duration.ofSeconds(8), "took the timeout path")
  }

  @Test
  fun `a timed-out connector's children are killed and the check returns`() {
    val childPid = captureDir.resolve("child.pid")

    val start = System.nanoTime()
    val result = check("sleep 600 & echo $! > $childPid; wait", checkTimeout = Duration.ofSeconds(1))

    assertEquals(SourceCheckResult.TimedOut, result)
    assertTrue(Duration.ofNanos(System.nanoTime() - start) < Duration.ofSeconds(5), "waited on the child's pipes")
    val child = ProcessHandle.of(Files.readString(childPid).trim().toLong())
    assertFalse(child.map { it.isAlive }.orElse(false), "the connector's child is still running")
  }
}
