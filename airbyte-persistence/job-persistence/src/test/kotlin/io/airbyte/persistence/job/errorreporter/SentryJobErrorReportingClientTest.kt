/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.sentry.IHub
import io.sentry.NoOpHub
import io.sentry.Scope
import io.sentry.ScopeCallback
import io.sentry.SentryEvent
import io.sentry.protocol.SentryException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.util.Map
import java.util.Optional
import java.util.UUID

internal class SentryJobErrorReportingClientTest {
  private val objectMapper = ObjectMapper()
  private val workspace: StandardWorkspace? = StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME)
  private lateinit var sentryErrorReportingClient: SentryJobErrorReportingClient
  private lateinit var mockSentryHub: IHub
  private lateinit var mockScope: Scope

  private var mockSentryExceptionHelper: SentryExceptionHelper? = null

  @BeforeEach
  fun setup() {
    mockSentryHub = mock<IHub>()
    mockScope = mock<Scope>()
    doAnswer { invocation: InvocationOnMock ->
      val scopeCallback = invocation.getArgument<ScopeCallback>(0)
      scopeCallback.run(mockScope)
      null // Return null for void methods
    }.whenever(mockSentryHub).configureScope(any<ScopeCallback>())

    mockSentryExceptionHelper = mock<SentryExceptionHelper>()
    sentryErrorReportingClient = SentryJobErrorReportingClient(mockSentryHub, mockSentryExceptionHelper!!)
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
    Assertions.assertEquals(sentryDSN, sentryHub.getOptions().getDsn())
    Assertions.assertFalse(sentryHub.getOptions().isAttachStacktrace())
    Assertions.assertFalse(sentryHub.getOptions().isEnableUncaughtExceptionHandler())
  }

  @Test
  @Throws(Exception::class)
  fun testReportJobFailureReason() {
    val eventCaptor = ArgumentCaptor.forClass<SentryEvent?, SentryEvent?>(SentryEvent::class.java)

    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
    val metadata = Map.of<String?, String?>("some_metadata", "some_metadata_value")
    val objectMapper = ObjectMapper()
    val sourceConfig = objectMapper.readTree("{\"sourceKey\": \"sourceValue\"}")
    val destinationConfig = objectMapper.readTree("{\"destinationKey\": \"destinationValue\"}")
    val state = State()
    state.withAdditionalProperty("stateKey", "stateValue")

    val attemptConfig = AttemptConfigReportingContext(sourceConfig, destinationConfig, state)

    sentryErrorReportingClient!!.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, metadata, attemptConfig)

    verify(mockSentryHub).captureEvent(eventCaptor.capture())
    val actualEvent = eventCaptor.getValue()
    Assertions.assertEquals("other", actualEvent.getPlatform())
    Assertions.assertEquals("airbyte-source-stripe@1.2.3", actualEvent.getRelease())
    Assertions.assertEquals(mutableListOf<String?>("{{ default }}", "airbyte-source-stripe"), actualEvent.getFingerprints())
    Assertions.assertEquals("some_metadata_value", actualEvent.getTag("some_metadata"))
    Assertions.assertNull(actualEvent.getTag(SentryJobErrorReportingClient.Companion.STACKTRACE_PARSE_ERROR_TAG_KEY))
    Assertions.assertNull(actualEvent.getExceptions())

    val contexts = actualEvent.getContexts()
    Assertions.assertNotNull(contexts)
    Assertions.assertTrue(contexts.containsKey("Failure Reason"))
    Assertions.assertTrue(contexts.containsKey("Source Configuration"))
    Assertions.assertTrue(contexts.containsKey("Destination Configuration"))
    Assertions.assertTrue(contexts.containsKey("State"))

    val sentryUser = actualEvent.getUser()
    Assertions.assertNotNull(sentryUser)
    Assertions.assertEquals(WORKSPACE_ID.toString(), sentryUser!!.getId())
    Assertions.assertEquals(WORKSPACE_NAME, sentryUser.getUsername())

    val message = actualEvent.getMessage()
    Assertions.assertNotNull(message)
    Assertions.assertEquals(ERROR_MESSAGE, message!!.getFormatted())
  }

  @Test
  fun testReportJobNoErrorOnNullAttemptConfig() {
    val eventCaptor = ArgumentCaptor.forClass<SentryEvent?, SentryEvent?>(SentryEvent::class.java)

    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
    val metadata = Map.of<String?, String?>("some_metadata", "some_metadata_value")

    sentryErrorReportingClient!!.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, metadata, null)

    verify(mockSentryHub).captureEvent(eventCaptor.capture())
  }

  @Test
  fun testReportJobFailureReasonWithNoWorkspace() {
    val eventCaptor = ArgumentCaptor.forClass<SentryEvent?, SentryEvent?>(SentryEvent::class.java)

    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    sentryErrorReportingClient!!.reportJobFailureReason(null, failureReason, DOCKER_IMAGE, Map.of<String?, String?>(), attemptConfig)

    verify(mockSentryHub).captureEvent(eventCaptor.capture())
    val actualEvent = eventCaptor.getValue()
    val sentryUser = actualEvent.getUser()
    Assertions.assertNull(sentryUser)

    val message = actualEvent.getMessage()
    Assertions.assertNotNull(message)
    Assertions.assertEquals(ERROR_MESSAGE, message!!.getFormatted())
  }

  @Test
  fun testReportJobFailureReasonWithStacktrace() {
    val eventCaptor = ArgumentCaptor.forClass<SentryEvent?, SentryEvent?>(SentryEvent::class.java)

    val exceptions = mutableListOf<SentryException>()
    val exception = SentryException()
    exception.setType("RuntimeError")
    exception.setValue("Something went wrong")
    exceptions.add(exception)

    val parsedException = SentryParsedException(SentryExceptionPlatform.PYTHON, exceptions)
    whenever(mockSentryExceptionHelper!!.buildSentryExceptions("Some valid stacktrace")).thenReturn(
      Optional.of(parsedException),
    )

    val failureReason =
      FailureReason()
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
        .withStacktrace("Some valid stacktrace")
    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    sentryErrorReportingClient!!.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, Map.of<String?, String?>(), attemptConfig)

    verify(mockSentryHub).captureEvent(eventCaptor.capture())
    val actualEvent = eventCaptor.getValue()
    Assertions.assertEquals(exceptions, actualEvent.getExceptions())
    Assertions.assertNull(actualEvent.getTag(SentryJobErrorReportingClient.Companion.STACKTRACE_PARSE_ERROR_TAG_KEY))
    Assertions.assertEquals("python", actualEvent.getPlatform())
    Assertions.assertEquals("python", actualEvent.getTag(SentryJobErrorReportingClient.Companion.STACKTRACE_PLATFORM_TAG_KEY))
  }

  @Test
  fun testReportJobFailureReasonWithInvalidStacktrace() {
    val eventCaptor = ArgumentCaptor.forClass<SentryEvent?, SentryEvent?>(SentryEvent::class.java)
    val invalidStacktrace = "Invalid stacktrace\nRuntimeError: Something went wrong"

    whenever(mockSentryExceptionHelper!!.buildSentryExceptions(invalidStacktrace))
      .thenReturn(Optional.empty())

    val failureReason =
      FailureReason()
        .withInternalMessage("Something went wrong")
        .withTimestamp(System.currentTimeMillis())
        .withStacktrace(invalidStacktrace)
    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    sentryErrorReportingClient!!.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, Map.of<String?, String?>(), attemptConfig)

    verify(mockSentryHub).captureEvent(eventCaptor.capture())
    val actualEvent = eventCaptor.getValue()
    Assertions.assertEquals("1", actualEvent.getTag(SentryJobErrorReportingClient.Companion.STACKTRACE_PARSE_ERROR_TAG_KEY))
    val exceptions = actualEvent.getExceptions()
    Assertions.assertNotNull(exceptions)
    Assertions.assertEquals(1, exceptions!!.size)
    Assertions.assertEquals("Invalid stacktrace, RuntimeError: ", exceptions.get(0)!!.getValue())
  }

  @Test
  fun testEmptyJsonNode() {
    val node: JsonNode = objectMapper.createObjectNode()
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(0, flatMap.size)
  }

  @Test
  @Throws(Exception::class)
  fun testSimpleFlatJson() {
    val node = objectMapper.readTree("{\"key1\":\"value1\", \"key2\":\"value2\"}")
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(2, flatMap.size)
    Assertions.assertEquals("value1", flatMap.get("key1"))
    Assertions.assertEquals("value2", flatMap.get("key2"))
  }

  @Test
  @Throws(Exception::class)
  fun testJsonWithArray() {
    val node = objectMapper.readTree("{\"a\": { \"b\": [{\"c\": 1}, {\"c\": 2}]}}")
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(2, flatMap.size)
    Assertions.assertEquals("1", flatMap.get("a.b[0].c"))
    Assertions.assertEquals("2", flatMap.get("a.b[1].c"))
  }

  @Test
  @Throws(Exception::class)
  fun testJsonWithNestedObject() {
    val node =
      objectMapper.readTree(
        "{\"key1\":\"value1\", \"nestedObject\":{\"nestedKey1\":\"nestedValue1\", \"nestedKey2\":\"nestedValue2\"}}",
      )
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(3, flatMap.size)
    Assertions.assertEquals("value1", flatMap.get("key1"))
    Assertions.assertEquals("nestedValue1", flatMap.get("nestedObject.nestedKey1"))
    Assertions.assertEquals("nestedValue2", flatMap.get("nestedObject.nestedKey2"))
  }

  @Test
  @Throws(Exception::class)
  fun testJsonWithNestedObjectsAndArray() {
    val node =
      objectMapper.readTree(
        "{\"key1\":\"value1\", \"nested\":{\"nestedKey1\":\"nestedValue1\", \"array\":[{\"item\":\"value2\"}, {\"item\":\"value3\"}]}}",
      )
    val flatMap = mutableMapOf<String, String>()
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap)

    Assertions.assertEquals(4, flatMap.size)
    Assertions.assertEquals("value1", flatMap.get("key1"))
    Assertions.assertEquals("nestedValue1", flatMap.get("nested.nestedKey1"))
    Assertions.assertEquals("value2", flatMap.get("nested.array[0].item"))
    Assertions.assertEquals("value3", flatMap.get("nested.array[1].item"))
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val WORKSPACE_NAME = "My Workspace"
    private const val DOCKER_IMAGE = "airbyte/source-stripe:1.2.3"
    private const val ERROR_MESSAGE = "RuntimeError: Something went wrong"
  }
}
