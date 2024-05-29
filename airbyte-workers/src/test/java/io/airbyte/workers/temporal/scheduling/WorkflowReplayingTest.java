/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.MapperFeature;
import com.google.common.base.Defaults;
import io.airbyte.commons.temporal.converter.AirbyteTemporalDataConverter;
import io.airbyte.micronaut.temporal.TemporalProxyHelper;
import io.airbyte.workers.temporal.sync.SyncWorkflowImpl;
import io.micronaut.context.BeanRegistration;
import io.micronaut.inject.BeanIdentifier;
import io.temporal.activity.ActivityOptions;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.common.RetryOptions;
import io.temporal.common.converter.ByteArrayPayloadConverter;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.DataConverterException;
import io.temporal.common.converter.EncodingKeys;
import io.temporal.common.converter.GlobalDataConverter;
import io.temporal.common.converter.JacksonJsonPayloadConverter;
import io.temporal.common.converter.NullPayloadConverter;
import io.temporal.common.converter.PayloadConverter;
import io.temporal.common.converter.ProtobufJsonPayloadConverter;
import io.temporal.common.converter.ProtobufPayloadConverter;
import io.temporal.testing.WorkflowReplayer;
import java.io.File;
import java.lang.reflect.Type;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// TODO: Auto generation of the input and more scenario coverage
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class WorkflowReplayingTest {

  private TemporalProxyHelper temporalProxyHelper;

  @BeforeAll
  static void beforeAll() {
    // Register the custom data converter configured to work with Airbyte JSON
    GlobalDataConverter.register(new AirbyteTemporalDataConverter());
  }

  @BeforeEach
  void setUp() {
    ActivityOptions activityOptions = ActivityOptions.newBuilder()
        .setHeartbeatTimeout(Duration.ofSeconds(30))
        .setStartToCloseTimeout(Duration.ofSeconds(120))
        .setRetryOptions(RetryOptions.newBuilder()
            .setMaximumAttempts(5)
            .setInitialInterval(Duration.ofSeconds(30))
            .setMaximumInterval(Duration.ofSeconds(600))
            .build())
        .build();

    final BeanRegistration shortActivityOptionsBeanRegistration = getActivityOptionBeanRegistration("shortActivityOptions", activityOptions);
    final BeanRegistration longActivityOptionsBeanRegistration = getActivityOptionBeanRegistration("longRunActivityOptions", activityOptions);
    final BeanRegistration discoveryActivityOptionsBeanRegistration = getActivityOptionBeanRegistration("discoveryActivityOptions", activityOptions);
    final BeanRegistration refreshSchemaActivityOptionsBeanRegistration =
        getActivityOptionBeanRegistration("refreshSchemaActivityOptions", activityOptions);

    temporalProxyHelper = new TemporalProxyHelper(
        List.of(shortActivityOptionsBeanRegistration, longActivityOptionsBeanRegistration, discoveryActivityOptionsBeanRegistration,
            refreshSchemaActivityOptionsBeanRegistration));
  }

  @Test
  void replaySimpleSuccessfulConnectionManagerWorkflow() throws Exception {
    // This test ensures that a new version of the workflow doesn't break an in-progress execution
    // This JSON file is exported from Temporal directly (e.g.
    // `http://${temporal-ui}/namespaces/default/workflows/connection_manager_-${uuid}/${uuid}/history`)
    // and export
    final URL historyPath = getClass().getClassLoader().getResource("connectionManagerWorkflowHistory.json");

    final File historyFile = new File(historyPath.toURI());

    WorkflowReplayer.replayWorkflowExecution(historyFile, temporalProxyHelper.proxyWorkflowClass(ConnectionManagerWorkflowImpl.class));
  }

  @Test
  void replaySyncWorkflowWithNormalization() throws Exception {
    // This test ensures that a new version of the workflow doesn't break an in-progress execution
    // This JSON file is exported from Temporal directly (e.g.
    // `http://${temporal-ui}/namespaces/default/workflows/connection_manager_-${uuid}/${uuid}/history`)
    // and export
    GlobalDataConverter.register(new TestPayloadConverter());
    final URL historyPath = getClass().getClassLoader().getResource("syncWorkflowHistory.json");
    final File historyFile = new File(historyPath.toURI());
    WorkflowReplayer.replayWorkflowExecution(historyFile, temporalProxyHelper.proxyWorkflowClass(SyncWorkflowImpl.class));
  }

  private BeanRegistration getActivityOptionBeanRegistration(String name, ActivityOptions activityOptions) {
    final BeanIdentifier activitiesBeanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration activityOptionsBeanRegistration = mock(BeanRegistration.class);
    when(activitiesBeanIdentifier.getName()).thenReturn(name);
    when(activityOptionsBeanRegistration.getIdentifier()).thenReturn(activitiesBeanIdentifier);
    when(activityOptionsBeanRegistration.getBean()).thenReturn(activityOptions);

    return activityOptionsBeanRegistration;
  }

  /**
   * Custom Temporal {@link DataConverter} that modifies the Jackson-based converter to enable case
   * insensitive enum value parsing by Jackson when loading a job history as part of the test.
   */
  private class TestPayloadConverter implements DataConverter {

    private static final PayloadConverter[] STANDARD_PAYLOAD_CONVERTERS = {
      new NullPayloadConverter(),
      new ByteArrayPayloadConverter(),
      new ProtobufJsonPayloadConverter(),
      new ProtobufPayloadConverter(),
      new JacksonJsonPayloadConverter(JacksonJsonPayloadConverter.newDefaultObjectMapper()
          .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS))
    };

    @Override
    public <T> Optional<Payload> toPayload(T value) throws DataConverterException {
      for (PayloadConverter converter : STANDARD_PAYLOAD_CONVERTERS) {
        Optional<Payload> result = converter.toData(value);
        if (result.isPresent()) {
          return result;
        }
      }
      return Optional.empty();
    }

    @Override
    public <T> T fromPayload(Payload payload, Class<T> valueClass, Type valueType) throws DataConverterException {
      try {
        String encoding =
            payload.getMetadataOrThrow(EncodingKeys.METADATA_ENCODING_KEY).toString(UTF_8);
        Optional<PayloadConverter> converter =
            Stream.of(STANDARD_PAYLOAD_CONVERTERS).filter(c -> encoding.equalsIgnoreCase(c.getEncodingType())).findFirst();
        if (converter.isEmpty()) {
          throw new DataConverterException(
              "No PayloadConverter is registered for an encoding: " + encoding);
        }
        return converter.get().fromData(payload, valueClass, valueType);
      } catch (DataConverterException e) {
        throw e;
      } catch (Exception e) {
        throw new DataConverterException(payload, valueClass, e);
      }
    }

    @Override
    public Optional<Payloads> toPayloads(Object... values) throws DataConverterException {
      if (values == null || values.length == 0) {
        return Optional.empty();
      }
      try {
        Payloads.Builder result = Payloads.newBuilder();
        for (Object value : values) {
          result.addPayloads(toPayload(value).get());
        }
        return Optional.of(result.build());
      } catch (DataConverterException e) {
        throw e;
      } catch (Throwable e) {
        throw new DataConverterException(e);
      }
    }

    @Override
    public <T> T fromPayloads(int index, Optional<Payloads> content, Class<T> parameterType, Type genericParameterType)
        throws DataConverterException {
      if (!content.isPresent()) {
        return Defaults.defaultValue(parameterType);
      }
      int count = content.get().getPayloadsCount();
      // To make adding arguments a backwards compatible change
      if (index >= count) {
        return Defaults.defaultValue(parameterType);
      }
      return fromPayload(content.get().getPayloads(index), parameterType, genericParameterType);
    }

  }

}
