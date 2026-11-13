/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs
import io.airbyte.directjobrunner.config.CheckConnectorConfig
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.eclipse.aether.resolution.ArtifactResolutionException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.time.Duration

private const val SUCCEEDED = """{"type":"CONNECTION_STATUS","connectionStatus":{"status":"SUCCEEDED"}}"""

private const val IMAGE = "airbyte/source-postgres:3.8.10"

private val JAR: Path = Path.of("/cache/io/airbyte/connectors/source-postgres/3.8.10/source-postgres-3.8.10.jar")

/** Each test replaces `java` with a shell script, so no JVM, jar or database is involved. */
class DirectCheckRunnerTest {
  @TempDir
  lateinit var scriptDir: Path

  @TempDir
  lateinit var captureDir: Path

  @TempDir
  lateinit var cacheDir: Path

  private val connectionConfiguration = Jsons.jsonNode(mapOf("host" to "db.example.com", "password" to "hunter2"))

  // A check's own download lands in the directory it is given, like the real resolver; the prefetch uses resolve().
  private val downloadDirs = mutableListOf<Path>()
  private val resolver =
    mockk<ConnectorJarResolver> {
      every { resolve(any()) } returns JAR
      every { resolveInto(any(), any()) } answers {
        val dir = secondArg<Path>()
        downloadDirs.add(dir)
        dir.resolve("source-postgres-3.8.10.jar").also { Files.writeString(it, "jar") }
      }
    }
  private val jarCache = ConnectorJarCache(resolver)

  private fun runner(
    javaBody: String,
    checkTimeout: Duration = Duration.ofSeconds(10),
    edition: Configs.AirbyteEdition = Configs.AirbyteEdition.COMMUNITY,
    jarDownloadAllowance: Duration = Duration.ofSeconds(10),
  ): DirectCheckRunner {
    val java = scriptDir.resolve("java")
    Files.writeString(java, "#!/bin/bash\n$javaBody\n")
    Files.setPosixFilePermissions(java, PosixFilePermissions.fromString("rwx------"))

    val config =
      CheckConnectorConfig(
        javaExecutable = java.toString(),
        jarCacheDir = cacheDir.toString(),
        checkTimeout = checkTimeout,
        jarDownloadAllowance = jarDownloadAllowance,
      )
    return DirectCheckRunner(config, edition, jarCache, resolver)
  }

  private fun check(
    javaBody: String,
    checkTimeout: Duration = Duration.ofSeconds(10),
    edition: Configs.AirbyteEdition = Configs.AirbyteEdition.COMMUNITY,
  ): SourceCheckResult = runner(javaBody, checkTimeout, edition).check(IMAGE, connectionConfiguration, "workload-1").result

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
        [ "${'$'}{@: -3:1}" = "--check" ] && [ "${'$'}{@: -2:1}" = "--config" ] && [ "${'$'}{@: -1}" = "/dev/stdin" ] || exit 9
        cat /dev/stdin > $capturedConfig
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
  fun `the connector jar runs with the image's JVM options and a capped heap`() {
    jarCache.put("io.airbyte.connectors:source-postgres:3.8.10")
    val capturedArgs = captureDir.resolve("args")

    check("printf '%s\\n' \"$@\" > $capturedArgs; echo '$SUCCEEDED'")

    assertEquals(
      listOf(
        "-Xms64m",
        "-Xmx512m",
        "-XX:ActiveProcessorCount=2",
        "-XX:+ExitOnOutOfMemoryError",
        "-Dlog4j.encoder.byteBufferSize=32768",
        "-Dlog4j2.configurationFile=log4j2.xml",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "-jar",
        JAR.toString(),
        "--check",
        "--config",
        "/dev/stdin",
      ),
      Files.readAllLines(capturedArgs),
    )
  }

  @Test
  fun `the connector gets none of the runner's environment`() {
    val capturedEnv = captureDir.resolve("env")

    check("env > $capturedEnv; echo '$SUCCEEDED'")

    val env =
      Files
        .readAllLines(capturedEnv)
        .filter { it.contains('=') }
        .associate { it.substringBefore('=') to it.substringAfter('=') }
    assertFalse(env.containsKey("HOME"), "$env")
    assertFalse(env.containsKey("USER"), "$env")
    assertFalse(env.containsKey("JAVA_OPTS"), "$env")
  }

  @Test
  fun `a prefetched jar runs from the cache, without a download`() {
    jarCache.put("io.airbyte.connectors:source-postgres:3.8.10")
    val capturedArgs = captureDir.resolve("args")

    val run = runner("printf '%s\\n' \"$@\" > $capturedArgs; echo '$SUCCEEDED'").check(IMAGE, connectionConfiguration, "workload-1")

    assertEquals(JarSource.CACHED, run.jarSource)
    assertTrue(Files.readAllLines(capturedArgs).contains(JAR.toString()), "the cached jar wasn't run")
    verify(exactly = 0) { resolver.resolveInto(any(), any()) }
  }

  @Test
  fun `a jar that isn't cached is downloaded for the check, deleted after it, and not cached`() {
    val runner = runner("echo '$SUCCEEDED'")

    val first = runner.check(IMAGE, connectionConfiguration, "workload-1")
    val second = runner.check(IMAGE, connectionConfiguration, "workload-2")

    assertEquals(listOf(JarSource.DOWNLOADED, JarSource.DOWNLOADED), listOf(first.jarSource, second.jarSource))
    verify(exactly = 2) { resolver.resolveInto("io.airbyte.connectors:source-postgres:3.8.10", any()) }
    assertEquals(2, downloadDirs.distinct().size, "$downloadDirs")
    downloadDirs.forEach { assertTrue(it.startsWith(cacheDir.resolve("check-downloads")), "a check's download is outside the cache dir: $it") }
    downloadDirs.forEach { assertFalse(Files.exists(it), "a check's download wasn't deleted: $it") }
    assertEquals(emptySet<String>(), jarCache.cached())
  }

  @Test
  fun `downloads left by checks that died with the last runner are removed when the runner starts`() {
    val leftover = cacheDir.resolve("check-downloads/check-123/source-postgres-3.8.10.jar")
    Files.createDirectories(leftover.parent)
    Files.writeString(leftover, "jar")
    val cachedJar = cacheDir.resolve("io/airbyte/connectors/source-postgres/3.8.11/source-postgres-3.8.11.jar")
    Files.createDirectories(cachedJar.parent)
    Files.writeString(cachedJar, "jar")

    runner("exit 0")

    assertFalse(Files.exists(cacheDir.resolve("check-downloads")), "a leftover check download survived the restart")
    assertTrue(Files.exists(cachedJar), "the startup cleanup touched the cache")
  }

  @Test
  fun `a check's download is deleted even when the connector fails`() {
    assertEquals(SourceCheckResult.Crashed(3), check("exit 3"))

    downloadDirs.forEach { assertFalse(Files.exists(it), "a check's download wasn't deleted: $it") }
  }

  @Test
  fun `the jar retrieval and the connector run are timed separately`() {
    every { resolver.resolveInto(any(), any()) } answers {
      Thread.sleep(300)
      JAR
    }

    val run = runner("sleep 0.5; echo '$SUCCEEDED'").check(IMAGE, connectionConfiguration, "workload-1")

    assertTrue(run.jarMs in 300..<500, "jarMs=${run.jarMs}")
    assertTrue(run.processMs >= 500, "processMs=${run.processMs}")
  }

  @Test
  fun `a download that takes longer than the allowance fails the check promptly and is cleaned up`() {
    every { resolver.resolveInto(any(), any()) } answers {
      downloadDirs.add(secondArg())
      Thread.sleep(5_000)
      JAR
    }
    val runner = runner("echo '$SUCCEEDED'", jarDownloadAllowance = Duration.ofMillis(300))

    val start = System.nanoTime()
    val e = assertThrows<IllegalStateException> { runner.check(IMAGE, connectionConfiguration, "workload-1") }

    assertTrue(Duration.ofNanos(System.nanoTime() - start) < Duration.ofSeconds(3), "waited for the whole download")
    assertEquals("Downloading io.airbyte.connectors:source-postgres:3.8.10 for the check took longer than PT0.3S", e.message)
    downloadDirs.forEach { assertFalse(Files.exists(it), "a check's download wasn't deleted: $it") }
  }

  @Test
  fun `a jar that can't be downloaded is a platform error, not a check result`() {
    every { resolver.resolveInto(any(), any()) } throws ArtifactResolutionException(emptyList(), "repository down")

    assertThrows<IllegalStateException> { check("echo '$SUCCEEDED'") }
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
    val runner = runner("head -c 204800 /dev/zero | tr '\\0' 'a' >&2\ncat > /dev/null\necho '$SUCCEEDED'")

    val start = System.nanoTime()
    val result = runner.check(IMAGE, bigConfig, "workload-1").result

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
