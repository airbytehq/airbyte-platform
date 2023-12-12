/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import static io.airbyte.persistence.job.errorreporter.SentryJobErrorReportingClient.STACKTRACE_PARSE_ERROR_TAG_KEY;
import static io.airbyte.persistence.job.errorreporter.SentryJobErrorReportingClient.STACKTRACE_PLATFORM_TAG_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.State;
import io.airbyte.persistence.job.errorreporter.SentryExceptionHelper.SentryExceptionPlatform;
import io.airbyte.persistence.job.errorreporter.SentryExceptionHelper.SentryParsedException;
import io.sentry.IHub;
import io.sentry.NoOpHub;
import io.sentry.Scope;
import io.sentry.ScopeCallback;
import io.sentry.SentryEvent;
import io.sentry.protocol.Message;
import io.sentry.protocol.SentryException;
import io.sentry.protocol.User;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class SentryJobErrorReportingClientTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String WORKSPACE_NAME = "My Workspace";
  private static final String DOCKER_IMAGE = "airbyte/source-stripe:1.2.3";
  private static final String ERROR_MESSAGE = "RuntimeError: Something went wrong";

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final StandardWorkspace workspace = new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME);
  private SentryJobErrorReportingClient sentryErrorReportingClient;
  private IHub mockSentryHub;
  private Scope mockScope;

  private SentryExceptionHelper mockSentryExceptionHelper;

  @BeforeEach
  void setup() {
    mockSentryHub = mock(IHub.class);
    mockScope = mock(Scope.class);
    doAnswer(invocation -> {
      final ScopeCallback scopeCallback = invocation.getArgument(0);
      scopeCallback.run(mockScope);
      return null; // Return null for void methods
    }).when(mockSentryHub).configureScope(any(ScopeCallback.class));

    mockSentryExceptionHelper = mock(SentryExceptionHelper.class);
    sentryErrorReportingClient = new SentryJobErrorReportingClient(mockSentryHub, mockSentryExceptionHelper);
  }

  @Test
  void testCreateSentryHubWithBlankDSN() {
    final String sentryDSN = "";
    final IHub sentryHub = SentryJobErrorReportingClient.createSentryHubWithDSN(sentryDSN);
    assertEquals(NoOpHub.getInstance(), sentryHub);
  }

  @Test
  void testCreateSentryHubWithNullDSN() {
    final IHub sentryHub = SentryJobErrorReportingClient.createSentryHubWithDSN(null);
    assertEquals(NoOpHub.getInstance(), sentryHub);
  }

  @Test
  void testCreateSentryHubWithDSN() {
    final String sentryDSN = "https://public@sentry.example.com/1";
    final IHub sentryHub = SentryJobErrorReportingClient.createSentryHubWithDSN(sentryDSN);
    assertNotNull(sentryHub);
    assertEquals(sentryDSN, sentryHub.getOptions().getDsn());
    assertFalse(sentryHub.getOptions().isAttachStacktrace());
    assertFalse(sentryHub.getOptions().isEnableUncaughtExceptionHandler());
  }

  @Test
  void testReportJobFailureReason() throws Exception {
    final ArgumentCaptor<SentryEvent> eventCaptor = ArgumentCaptor.forClass(SentryEvent.class);

    final FailureReason failureReason = new FailureReason()
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis());
    final Map<String, String> metadata = Map.of("some_metadata", "some_metadata_value");
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonNode sourceConfig = objectMapper.readTree("{\"sourceKey\": \"sourceValue\"}");
    final JsonNode destinationConfig = objectMapper.readTree("{\"destinationKey\": \"destinationValue\"}");
    final State state = new State();
    state.withAdditionalProperty("stateKey", "stateValue");

    final AttemptConfigReportingContext attemptConfig = new AttemptConfigReportingContext(sourceConfig, destinationConfig, state);

    sentryErrorReportingClient.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, metadata, attemptConfig);

    verify(mockSentryHub).captureEvent(eventCaptor.capture());
    final SentryEvent actualEvent = eventCaptor.getValue();
    assertEquals("other", actualEvent.getPlatform());
    assertEquals("airbyte-source-stripe@1.2.3", actualEvent.getRelease());
    assertEquals(List.of("{{ default }}", "airbyte-source-stripe"), actualEvent.getFingerprints());
    assertEquals("some_metadata_value", actualEvent.getTag("some_metadata"));
    assertNull(actualEvent.getTag(STACKTRACE_PARSE_ERROR_TAG_KEY));
    assertNull(actualEvent.getExceptions());

    // verify that scope setContexts is called with the correct arguments
    verify(mockScope).setContexts(Mockito.eq("Failure Reason"), any(Object.class));
    verify(mockScope).setContexts(Mockito.eq("Source Configuration"), any(Object.class));
    verify(mockScope).setContexts(Mockito.eq("Destination Configuration"), any(Object.class));
    verify(mockScope).setContexts(Mockito.eq("State"), any(Object.class));

    final User sentryUser = actualEvent.getUser();
    assertNotNull(sentryUser);
    assertEquals(WORKSPACE_ID.toString(), sentryUser.getId());
    assertEquals(WORKSPACE_NAME, sentryUser.getUsername());

    final Message message = actualEvent.getMessage();
    assertNotNull(message);
    assertEquals(ERROR_MESSAGE, message.getFormatted());
  }

  @Test
  void testReportJobNoErrorOnNullAttemptConfig() {
    final ArgumentCaptor<SentryEvent> eventCaptor = ArgumentCaptor.forClass(SentryEvent.class);

    final FailureReason failureReason = new FailureReason()
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis());
    final Map<String, String> metadata = Map.of("some_metadata", "some_metadata_value");

    sentryErrorReportingClient.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, metadata, null);

    verify(mockSentryHub).captureEvent(eventCaptor.capture());
  }

  @Test
  void testReportJobFailureReasonWithNoWorkspace() {
    final ArgumentCaptor<SentryEvent> eventCaptor = ArgumentCaptor.forClass(SentryEvent.class);

    final FailureReason failureReason = new FailureReason()
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR)
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis());
    final ObjectMapper objectMapper = new ObjectMapper();
    final AttemptConfigReportingContext attemptConfig =
        new AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), new State());

    sentryErrorReportingClient.reportJobFailureReason(null, failureReason, DOCKER_IMAGE, Map.of(), attemptConfig);

    verify(mockSentryHub).captureEvent(eventCaptor.capture());
    final SentryEvent actualEvent = eventCaptor.getValue();
    final User sentryUser = actualEvent.getUser();
    assertNull(sentryUser);

    final Message message = actualEvent.getMessage();
    assertNotNull(message);
    assertEquals(ERROR_MESSAGE, message.getFormatted());
  }

  @Test
  void testReportJobFailureReasonWithStacktrace() {
    final ArgumentCaptor<SentryEvent> eventCaptor = ArgumentCaptor.forClass(SentryEvent.class);

    final List<SentryException> exceptions = new ArrayList<>();
    final SentryException exception = new SentryException();
    exception.setType("RuntimeError");
    exception.setValue("Something went wrong");
    exceptions.add(exception);

    final SentryParsedException parsedException = new SentryParsedException(SentryExceptionPlatform.PYTHON, exceptions);
    when(mockSentryExceptionHelper.buildSentryExceptions("Some valid stacktrace")).thenReturn(Optional.of(parsedException));

    final FailureReason failureReason = new FailureReason()
        .withInternalMessage(ERROR_MESSAGE)
        .withTimestamp(System.currentTimeMillis())
        .withStacktrace("Some valid stacktrace");
    final ObjectMapper objectMapper = new ObjectMapper();
    final AttemptConfigReportingContext attemptConfig =
        new AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), new State());

    sentryErrorReportingClient.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, Map.of(), attemptConfig);

    verify(mockSentryHub).captureEvent(eventCaptor.capture());
    final SentryEvent actualEvent = eventCaptor.getValue();
    assertEquals(exceptions, actualEvent.getExceptions());
    assertNull(actualEvent.getTag(STACKTRACE_PARSE_ERROR_TAG_KEY));
    assertEquals("python", actualEvent.getPlatform());
    assertEquals("python", actualEvent.getTag(STACKTRACE_PLATFORM_TAG_KEY));
  }

  @Test
  void testReportJobFailureReasonWithInvalidStacktrace() {
    final ArgumentCaptor<SentryEvent> eventCaptor = ArgumentCaptor.forClass(SentryEvent.class);
    final String invalidStacktrace = "Invalid stacktrace\nRuntimeError: Something went wrong";

    when(mockSentryExceptionHelper.buildSentryExceptions(invalidStacktrace)).thenReturn(Optional.empty());

    final FailureReason failureReason = new FailureReason()
        .withInternalMessage("Something went wrong")
        .withTimestamp(System.currentTimeMillis())
        .withStacktrace(invalidStacktrace);
    final ObjectMapper objectMapper = new ObjectMapper();
    final AttemptConfigReportingContext attemptConfig =
        new AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), new State());

    sentryErrorReportingClient.reportJobFailureReason(workspace, failureReason, DOCKER_IMAGE, Map.of(), attemptConfig);

    verify(mockSentryHub).captureEvent(eventCaptor.capture());
    final SentryEvent actualEvent = eventCaptor.getValue();
    assertEquals("1", actualEvent.getTag(STACKTRACE_PARSE_ERROR_TAG_KEY));
    final List<SentryException> exceptions = actualEvent.getExceptions();
    assertNotNull(exceptions);
    assertEquals(1, exceptions.size());
    assertEquals("Invalid stacktrace, RuntimeError: ", exceptions.get(0).getValue());
  }

  @Test
  void testEmptyJsonNode() {
    JsonNode node = objectMapper.createObjectNode();
    Map<String, String> flatMap = new HashMap<>();
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap);

    assertEquals(0, flatMap.size());
  }

  @Test
  void testSimpleFlatJson() throws Exception {
    JsonNode node = objectMapper.readTree("{\"key1\":\"value1\", \"key2\":\"value2\"}");
    Map<String, String> flatMap = new HashMap<>();
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap);

    assertEquals(2, flatMap.size());
    assertEquals("value1", flatMap.get("key1"));
    assertEquals("value2", flatMap.get("key2"));
  }

  @Test
  void testJsonWithArray() throws Exception {
    JsonNode node = objectMapper.readTree("{\"a\": { \"b\": [{\"c\": 1}, {\"c\": 2}]}}");
    Map<String, String> flatMap = new HashMap<>();
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap);

    assertEquals(2, flatMap.size());
    assertEquals("1", flatMap.get("a.b[0].c"));
    assertEquals("2", flatMap.get("a.b[1].c"));
  }

  @Test
  void testJsonWithNestedObject() throws Exception {
    JsonNode node = objectMapper.readTree(
        "{\"key1\":\"value1\", \"nestedObject\":{\"nestedKey1\":\"nestedValue1\", \"nestedKey2\":\"nestedValue2\"}}");
    Map<String, String> flatMap = new HashMap<>();
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap);

    assertEquals(3, flatMap.size());
    assertEquals("value1", flatMap.get("key1"));
    assertEquals("nestedValue1", flatMap.get("nestedObject.nestedKey1"));
    assertEquals("nestedValue2", flatMap.get("nestedObject.nestedKey2"));
  }

  @Test
  void testJsonWithNestedObjectsAndArray() throws Exception {
    JsonNode node = objectMapper.readTree(
        "{\"key1\":\"value1\", \"nested\":{\"nestedKey1\":\"nestedValue1\", \"array\":[{\"item\":\"value2\"}, {\"item\":\"value3\"}]}}");
    Map<String, String> flatMap = new HashMap<>();
    SentryJobErrorReportingClient.flattenJsonNode("", node, flatMap);

    assertEquals(4, flatMap.size());
    assertEquals("value1", flatMap.get("key1"));
    assertEquals("nestedValue1", flatMap.get("nested.nestedKey1"));
    assertEquals("value2", flatMap.get("nested.array[0].item"));
    assertEquals("value3", flatMap.get("nested.array[1].item"));
  }

}
