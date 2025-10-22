/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.unmockkAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStreamReader

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProfilerServiceTest {
  private val fileService = mockk<FileService>(relaxed = true)
  private val dummyFailSafeRetryPolicies: FailSafeRetryPolicies = DummyFailSafeRetryPolicies()
  private lateinit var testableService: TestableProfilerService

  private fun <T> withSystemProperties(
    properties: Map<String, String>,
    block: () -> T,
  ): T {
    val originalValues = properties.keys.associateWith { System.getProperty(it) }
    return try {
      properties.forEach { (key, value) ->
        System.setProperty(key, value)
      }
      block()
    } finally {
      originalValues.forEach { (key, originalValue) ->
        if (originalValue == null) {
          System.clearProperty(key)
        } else {
          System.setProperty(key, originalValue)
        }
      }
    }
  }

  private fun getOsTestProperties(): Map<String, String> =
    mapOf(
      "os.name" to "Linux",
      "os.arch" to "amd64",
    )

  @BeforeEach
  fun setUp() {
    testableService = TestableProfilerService(fileService, dummyFailSafeRetryPolicies)
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  /**
   * A test subclass of ProfilerService that overrides createProcessBuilder
   * to return JPS or Profiler mocks accordingly.
   */
  inner class TestableProfilerService(
    fileService: FileService,
    dummyFailSafeRetryPolicies: FailSafeRetryPolicies,
  ) : ProfilerService(fileService, dummyFailSafeRetryPolicies) {
    // We define 2 distinct mocks for each "phase":
    val jpsProcessBuilder = mockk<ProcessBuilder>(relaxed = true)
    val profilerProcessBuilder = mockk<ProcessBuilder>(relaxed = true)

    override fun createProcessBuilder(vararg command: String): ProcessBuilder =
      if (command.size == 2 && command[0] == "jps" && command[1] == "-l") {
        jpsProcessBuilder
      } else {
        // It's presumably the script command
        profilerProcessBuilder
      }
  }

  @Nested
  @DisplayName("ProfilerService tests")
  inner class RunProfilerTests {
    @Test
    fun `should fail if jps can't find the process`() {
      withSystemProperties(
        getOsTestProperties(),
      ) {
        // 1) Mock the jps process
        val jpsProcess = mockk<Process>(relaxed = true)

        every { jpsProcess.inputReader() } returns BufferedReader(InputStreamReader(ByteArrayInputStream(ByteArray(0))))

        // 2) When we call start() on jpsProcessBuilder, return the jpsProcess
        every { testableService.jpsProcessBuilder.start() } returns jpsProcess

        // 3) Now call runProfiler
        val result = testableService.runProfiler("myApp", "cpu")

        // 4) We expect a FAILED result
        Assertions.assertEquals(ProfilerThreadManager.ProfilerResult.FAILED, result.resultStatus)
        Assertions.assertTrue(result.message.contains("Could not find process containing 'myApp'"))
      }
    }

    @Test
    fun `should fail for unsupported OS or arc`() {
      withSystemProperties(
        mapOf("os.name" to "Windows", "os.arch" to "x86"),
      ) {
        // 1) Make sure jps finds a PID
        val jpsProcess = mockk<Process>(relaxed = true)
        val jpsOutput = "9999 com.myApp.MainClass\n"
        every { jpsProcess.inputReader() } returns BufferedReader(InputStreamReader(ByteArrayInputStream(jpsOutput.toByteArray())))

        every { testableService.jpsProcessBuilder.start() } returns jpsProcess

        val result = testableService.runProfiler("myApp", "cpu")
        Assertions.assertEquals(ProfilerThreadManager.ProfilerResult.FAILED, result.resultStatus)
        Assertions.assertTrue(result.message.contains("Unsupported OS/arch combination"))
      }
    }

    @Test
    fun `should succeed if profiler exits with code 0 and output file is found`() {
      withSystemProperties(
        getOsTestProperties(),
      ) {
        // 1) jps process sees PID 1234
        val jpsProcess = mockk<Process>(relaxed = true)
        val jpsOutput = "1234 com.myApp.MainClass\n"
        every { jpsProcess.inputReader() } returns BufferedReader(InputStreamReader(ByteArrayInputStream(jpsOutput.toByteArray())))
        every { testableService.jpsProcessBuilder.start() } returns jpsProcess

        // 2) FileService calls
        val tempDir = File("/tmp/")
        every { fileService.createTempDirectory(any()) } returns tempDir
        every { fileService.downloadFile(any(), any()) } just runs
        every { fileService.extractArchive(any()) } just runs

        val scriptFile = mockk<File>(relaxed = true)
        every { scriptFile.absolutePath } returns "/tmp/profiler.sh"
        every { fileService.findProfilerScript(any()) } returns scriptFile
        every { fileService.fileExists(any()) } returns true

        // 3) Profiler process
        val profilerProcess = mockk<Process>(relaxed = true)
        val profilerStdout = "Profiling started...\nProfiling finished."
        every { profilerProcess.inputStream } returns ByteArrayInputStream(profilerStdout.toByteArray())
        every { profilerProcess.errorStream } returns ByteArrayInputStream(ByteArray(0))
        every { profilerProcess.waitFor() } returns 0
        every { testableService.profilerProcessBuilder.start() } returns profilerProcess

        // 5) Run
        val result = testableService.runProfiler("myApp", "cpu")

        Assertions.assertEquals(ProfilerThreadManager.ProfilerResult.SUCCESS, result.resultStatus)
        Assertions.assertTrue(result.message.contains("Profiling succeeded!"))
      }
    }

    @Test
    fun `should fail if profiler exists with code 0 but no output file is found`() {
      withSystemProperties(
        getOsTestProperties(),
      ) {
        // jps sees PID
        val jpsProcess = mockk<Process>(relaxed = true)
        val jpsOutput = "1234 com.myApp.Main\n"
        every { jpsProcess.inputReader() } returns BufferedReader(InputStreamReader(ByteArrayInputStream(jpsOutput.toByteArray())))
        every { testableService.jpsProcessBuilder.start() } returns jpsProcess

        // fileService
        val tempDir = File("/tmp/")
        every { fileService.createTempDirectory(any()) } returns tempDir
        every { fileService.downloadFile(any(), any()) } just runs
        every { fileService.extractArchive(any()) } just runs
        every { fileService.fileExists(any()) } returns false

        val scriptFile = mockk<File>(relaxed = true)
        every { scriptFile.absolutePath } returns "/tmp/profiler.sh"
        every { fileService.findProfilerScript(any()) } returns scriptFile

        // profilerProcess => exit code 0
        val profilerProcess = mockk<Process>(relaxed = true)
        every { profilerProcess.waitFor() } returns 0
        every { testableService.profilerProcessBuilder.start() } returns profilerProcess

        val result = testableService.runProfiler("myApp", "cpu")
        Assertions.assertEquals(ProfilerThreadManager.ProfilerResult.FAILED, result.resultStatus)
        Assertions.assertTrue(result.message.contains("no output file found"))
      }
    }

    @Test
    fun `should fail if profiler exits with non-zero code`() {
      withSystemProperties(
        getOsTestProperties(),
      ) {
        val jpsProcess = mockk<Process>(relaxed = true)
        val jpsOutput = "1234 com.myApp.Main\n"
        every { jpsProcess.inputReader() } returns BufferedReader(InputStreamReader(ByteArrayInputStream(jpsOutput.toByteArray())))
        every { testableService.jpsProcessBuilder.start() } returns jpsProcess

        // fileService stubs
        val tempDir = File("/tmp/")
        every { fileService.createTempDirectory(any()) } returns tempDir
        every { fileService.downloadFile(any(), any()) } just runs
        every { fileService.extractArchive(any()) } just runs

        val scriptFile = mockk<File>(relaxed = true)
        every { scriptFile.absolutePath } returns "/tmp/profiler.sh"
        every { fileService.findProfilerScript(any()) } returns scriptFile

        // Non-zero exit code from profiler
        val profilerProcess = mockk<Process>(relaxed = true)
        every { profilerProcess.waitFor() } returns 7
        every { testableService.profilerProcessBuilder.start() } returns profilerProcess

        val result = testableService.runProfiler("myApp", "cpu")
        Assertions.assertEquals(ProfilerThreadManager.ProfilerResult.FAILED, result.resultStatus)
        Assertions.assertTrue(result.message.contains("exit code 7"))
      }
    }

    @Test
    fun `should handle exception gracefully`() {
      withSystemProperties(
        getOsTestProperties(),
      ) {
        // jps sees PID
        val jpsProcess = mockk<Process>(relaxed = true)
        val jpsOutput = "1234 com.myApp.Main\n"
        every { jpsProcess.inputReader() } returns BufferedReader(InputStreamReader(ByteArrayInputStream(jpsOutput.toByteArray())))
        every { testableService.jpsProcessBuilder.start() } returns jpsProcess

        // Throw an exception from downloadFile
        val tempDir = File("/tmp/")
        every { fileService.downloadFile(any(), any()) } throws RuntimeException("Network Error")
        every { fileService.createTempDirectory(any()) } returns tempDir

        val result = testableService.runProfiler("myApp", "cpu")
        Assertions.assertEquals(ProfilerThreadManager.ProfilerResult.FAILED, result.resultStatus)
        Assertions.assertTrue(result.message.contains("Network Error"))
      }
    }
  }
}
