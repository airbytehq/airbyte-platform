/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.State
import io.airbyte.persistence.job.errorreporter.SentryExceptionHelper.SentryExceptionPlatform
import io.airbyte.persistence.job.errorreporter.SentryExceptionHelper.SentryParsedException
import io.airbyte.persistence.job.errorreporter.SentryJobErrorReportingClient.Companion.createSentryHubWithDSN
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.sentry.IHub
import io.sentry.NoOpHub
import io.sentry.Scope
import io.sentry.ScopeCallback
import io.sentry.SentryEvent
import io.sentry.protocol.SentryException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

internal class SentryJobErrorReportingClientTest {
  private val objectMapper = ObjectMapper()
  private val workspace: StandardWorkspace = StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME)
  private lateinit var sentryErrorReportingClient: SentryJobErrorReportingClient
  private lateinit var mockSentryHub: IHub
  private lateinit var mockScope: Scope
  private lateinit var mockSentryExceptionHelper: SentryExceptionHelper

  @BeforeEach
  fun setup() {
    mockSentryHub = mockk<IHub>()
    mockScope = mockk<Scope>(relaxed = true)
    every { mockSentryHub.configureScope(any<ScopeCallback>()) } answers {
      val scopeCallback = arg<ScopeCallback>(0)
      scopeCallback.run(mockScope)
    }
    every { mockSentryHub.captureEvent(any<SentryEvent>()) } returns mockk()

    mockSentryExceptionHelper = mockk<SentryExceptionHelper>()
    sentryErrorReportingClient = SentryJobErrorReportingClient(mockSentryHub, mockSentryExceptionHelper)
  }

  @Test
  fun testCreateSentryHubWithBlankDSN() {
    val sentryDSN = ""
    val sentryHub = createSentryHubWithDSN(sentryDSN)
    Assertions.assertEquals(NoOpHub.getInstance(), sentryHub)
  }

  @Test
  fun testCreateSentryHubWithNullDSN() {
    val sentryHub = createSentryHubWithDSN(null)
    Assertions.assertEquals(NoOpHub.getInstance(), sentryHub)
  }

  @Test
  fun testCreateSentryHubWithDSN() {
    val sentryDSN = "https://public@sentry.example.com/1"
    val sentryHub = createSentryHubWithDSN(sentryDSN)
    Assertions.assertNotNull(sentryHub)
    Assertions.assertEquals(sentryDSN, sentryHub.options.dsn)
    Assertions.assertFalse(sentryHub.options.isAttachStacktrace)
    Assertions.assertFalse(sentryHub.options.isEnableUncaughtExceptionHandler)
  }

  @Test
  fun testReportJobFailureReason() {
    val eventCaptor = slot<SentryEvent>()

    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
    val metadata = mapOf<String?, String?>("some_metadata" to "some_metadata_value")
    val objectMapper = ObjectMapper()
    val sourceConfig = objectMapper.readTree("{\"sourceKey\": \"sourceValue\"}")
    val destinationConfig = objectMapper.readTree("{\"destinationKey\": \"destinationValue\"}")
    val state = State()
    state.withAdditionalProperty("stateKey", "stateValue")

    val attemptConfig = AttemptConfigReportingContext(sourceConfig, destinationConfig, state)

    sentryErrorReportingClient.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, metadata, attemptConfig)

    verify { mockSentryHub.captureEvent(capture(eventCaptor)) }
    val actualEvent = eventCaptor.captured
    Assertions.assertEquals("other", actualEvent.platform)
    Assertions.assertEquals("airbyte-source-stripe@1.2.3", actualEvent.release)
    Assertions.assertEquals(mutableListOf<String?>("{{ default }}", "airbyte-source-stripe"), actualEvent.fingerprints)
    Assertions.assertEquals("some_metadata_value", actualEvent.getTag("some_metadata"))
    Assertions.assertNull(actualEvent.getTag(SentryJobErrorReportingClient.STACKTRACE_PARSE_ERROR_TAG_KEY))
    Assertions.assertNull(actualEvent.exceptions)

    val contexts = actualEvent.contexts
    Assertions.assertNotNull(contexts)
    Assertions.assertTrue(contexts.containsKey("Failure Reason"))
    Assertions.assertTrue(contexts.containsKey("Source Configuration"))
    Assertions.assertTrue(contexts.containsKey("Destination Configuration"))
    Assertions.assertTrue(contexts.containsKey("State"))

    val sentryUser = actualEvent.user
    Assertions.assertNotNull(sentryUser)
    Assertions.assertEquals(WORKSPACE_ID.toString(), sentryUser!!.id)
    Assertions.assertEquals(WORKSPACE_NAME, sentryUser.username)

    val message = actualEvent.message
    Assertions.assertNotNull(message)
    Assertions.assertEquals(ERROR_MESSAGE, message!!.formatted)
  }

  @Test
  fun testReportJobNoErrorOnNullAttemptConfig() {
    val eventCaptor = slot<SentryEvent>()

    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
    val metadata = mapOf<String?, String?>("some_metadata" to "some_metadata_value")

    sentryErrorReportingClient.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, metadata, null)

    verify { mockSentryHub.captureEvent(capture(eventCaptor)) }
  }

  @Test
  fun testReportJobFailureReasonWithNoWorkspace() {
    val eventCaptor = slot<SentryEvent>()

    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    sentryErrorReportingClient.reportJobFailureReason(null, failureReason, DOCKER_IMAGE, emptyMap(), attemptConfig)

    verify { mockSentryHub.captureEvent(capture(eventCaptor)) }
    val actualEvent = eventCaptor.captured
    val sentryUser = actualEvent.user
    Assertions.assertNull(sentryUser)

    val message = actualEvent.message
    Assertions.assertNotNull(message)
    Assertions.assertEquals(ERROR_MESSAGE, message!!.formatted)
  }

  @Test
  fun testReportJobFailureReasonWithStacktrace() {
    val eventCaptor = slot<SentryEvent>()

    val exceptions = mutableListOf<SentryException>()
    val exception = SentryException()
    exception.type = "RuntimeError"
    exception.value = "Something went wrong"
    exceptions.add(exception)

    val parsedException = SentryParsedException(SentryExceptionPlatform.PYTHON, exceptions)
    every { mockSentryExceptionHelper.buildSentryExceptions("Some valid stacktrace") } returns Optional.of(parsedException)

    val failureReason =
      FailureReason()
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
        .withStacktrace("Some valid stacktrace")
    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    sentryErrorReportingClient.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, emptyMap(), attemptConfig)

    verify { mockSentryHub.captureEvent(capture(eventCaptor)) }
    val actualEvent = eventCaptor.captured
    Assertions.assertEquals(exceptions, actualEvent.exceptions)
    Assertions.assertNull(actualEvent.getTag(SentryJobErrorReportingClient.STACKTRACE_PARSE_ERROR_TAG_KEY))
    Assertions.assertEquals("python", actualEvent.platform)
    Assertions.assertEquals("python", actualEvent.getTag(SentryJobErrorReportingClient.STACKTRACE_PLATFORM_TAG_KEY))
  }

  @Test
  fun testReportJobFailureReasonWithInvalidStacktrace() {
    val eventCaptor = slot<SentryEvent>()
    val invalidStacktrace = "Invalid stacktrace\nRuntimeError: Something went wrong"

    every { mockSentryExceptionHelper.buildSentryExceptions(invalidStacktrace) } returns Optional.empty()

    val failureReason =
      FailureReason()
        .withInternalMessage("Something went wrong")
        .withTimestamp(System.currentTimeMillis())
        .withStacktrace(invalidStacktrace)
    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    sentryErrorReportingClient.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, emptyMap(), attemptConfig)

    verify { mockSentryHub.captureEvent(capture(eventCaptor)) }
    val actualEvent = eventCaptor.captured
    Assertions.assertEquals("1", actualEvent.getTag(SentryJobErrorReportingClient.STACKTRACE_PARSE_ERROR_TAG_KEY))
    val exceptions = actualEvent.exceptions
    Assertions.assertNotNull(exceptions)
    Assertions.assertEquals(1, exceptions!!.size)
    Assertions.assertEquals("Invalid stacktrace, RuntimeError: ", exceptions[0]!!.value)
  }

  @Test
  fun testEmptyJsonNode() {
    val node: JsonNode = objectMapper.createObjectNode()
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(0, flatMap.size)
  }

  @Test
  fun testSimpleFlatJson() {
    val node = objectMapper.readTree("{\"key1\":\"value1\", \"key2\":\"value2\"}")
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(2, flatMap.size)
    Assertions.assertEquals("value1", flatMap["key1"])
    Assertions.assertEquals("value2", flatMap["key2"])
  }

  @Test
  fun testJsonWithArray() {
    val node = objectMapper.readTree("{\"a\": { \"b\": [{\"c\": 1}, {\"c\": 2}]}}")
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(2, flatMap.size)
    Assertions.assertEquals("1", flatMap["a.b[0].c"])
    Assertions.assertEquals("2", flatMap["a.b[1].c"])
  }

  @Test
  fun testJsonWithNestedObject() {
    val node =
      objectMapper.readTree(
        "{\"key1\":\"value1\", \"nestedObject\":{\"nestedKey1\":\"nestedValue1\", \"nestedKey2\":\"nestedValue2\"}}",
      )
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(3, flatMap.size)
    Assertions.assertEquals("value1", flatMap["key1"])
    Assertions.assertEquals("nestedValue1", flatMap["nestedObject.nestedKey1"])
    Assertions.assertEquals("nestedValue2", flatMap["nestedObject.nestedKey2"])
  }

  @Test
  fun testJsonWithNestedObjectsAndArray() {
    val node =
      objectMapper.readTree(
        "{\"key1\":\"value1\", \"nested\":{\"nestedKey1\":\"nestedValue1\", \"array\":[{\"item\":\"value2\"}, {\"item\":\"value3\"}]}}",
      )
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(4, flatMap.size)
    Assertions.assertEquals("value1", flatMap["key1"])
    Assertions.assertEquals("nestedValue1", flatMap["nested.nestedKey1"])
    Assertions.assertEquals("value2", flatMap["nested.array[0].item"])
    Assertions.assertEquals("value3", flatMap["nested.array[1].item"])
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val WORKSPACE_NAME = "My Workspace"
    private const val DOCKER_IMAGE = "airbyte/source-stripe:1.2.3"
    private const val ERROR_MESSAGE = "RuntimeError: Something went wrong"
  }
}
