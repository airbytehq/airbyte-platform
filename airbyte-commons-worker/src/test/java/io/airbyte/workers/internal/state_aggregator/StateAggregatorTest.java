/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.state_aggregator;

import static io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType.GLOBAL;
import static io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType.LEGACY;
import static io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType.STREAM;

import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.State;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.StreamDescriptor;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.CollectionAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class StateAggregatorTest {

  StateAggregator stateAggregator;
  private static final boolean USE_STREAM_CAPABLE_STATE = true;
  private static final boolean DONT_USE_STREAM_CAPABLE_STATE = false;

  @BeforeEach
  void init() {
    stateAggregator = new DefaultStateAggregator(DONT_USE_STREAM_CAPABLE_STATE);
  }

  @ParameterizedTest
  @EnumSource(AirbyteStateType.class)
  void testCantMixType(final AirbyteStateType stateType) {
    final Stream<AirbyteStateType> allTypes = Arrays.stream(AirbyteStateType.values());

    stateAggregator.ingest(getEmptyMessage(stateType));

    final List<AirbyteStateType> differentTypes = allTypes.filter(type -> type != stateType).toList();
    differentTypes.forEach(differentType -> Assertions.assertThatThrownBy(() -> stateAggregator.ingest(getEmptyMessage(differentType))));
  }

  @Test
  void testCantMixNullType() {
    final List<AirbyteStateType> allIncompatibleTypes = Lists.newArrayList(GLOBAL, STREAM);

    stateAggregator.ingest(getEmptyMessage(null));

    allIncompatibleTypes.forEach(differentType -> Assertions.assertThatThrownBy(() -> stateAggregator.ingest(getEmptyMessage(differentType))));

    stateAggregator.ingest(getEmptyMessage(LEGACY));
  }

  @Test
  void testNullState() {
    final AirbyteStateMessage state1 = getNullMessage(1);
    final AirbyteStateMessage state2 = getNullMessage(2);

    stateAggregator.ingest(state1);
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(state1.getData()));

    stateAggregator.ingest(state2);
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(state2.getData()));
  }

  @Test
  void testLegacyState() {
    final AirbyteStateMessage state1 = getLegacyMessage(1);
    final AirbyteStateMessage state2 = getLegacyMessage(2);

    stateAggregator.ingest(state1);
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(state1.getData()));

    stateAggregator.ingest(state2);
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(state2.getData()));
  }

  @Test
  void testGlobalState() {
    final AirbyteStateMessage state1 = getGlobalMessage(1);
    final AirbyteStateMessage state2 = getGlobalMessage(2);

    final AirbyteStateMessage state1NoData = getGlobalMessage(1).withData(null);
    final AirbyteStateMessage state2NoData = getGlobalMessage(2).withData(null);

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state1), AirbyteStateMessage.class));
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(Jsons.jsonNode(List.of(state1NoData))));

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state2), AirbyteStateMessage.class));
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(Jsons.jsonNode(List.of(state2NoData))));
  }

  @Test
  void testStreamStateWithFeatureFlagOff() {
    final AirbyteStateMessage state1 = getStreamMessage("a", 1);
    final AirbyteStateMessage state2 = getStreamMessage("b", 2);
    final AirbyteStateMessage state3 = getStreamMessage("b", 3);

    stateAggregator.ingest(state1);
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(Jsons.jsonNode(List.of(state1))));

    stateAggregator.ingest(state2);
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(Jsons.jsonNode(List.of(state2))));

    stateAggregator.ingest(state3);
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(Jsons.jsonNode(List.of(state3))));
  }

  @Test
  void testStreamStateWithFeatureFlagOn() {
    final AirbyteStateMessage state1 = getStreamMessage("a", 1);
    final AirbyteStateMessage state2 = getStreamMessage("b", 2);
    final AirbyteStateMessage state3 = getStreamMessage("b", 3);

    final AirbyteStateMessage state1NoData = getStreamMessage("a", 1).withData(null);
    final AirbyteStateMessage state2NoData = getStreamMessage("b", 2).withData(null);
    final AirbyteStateMessage state3NoData = getStreamMessage("b", 3).withData(null);

    stateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state1), AirbyteStateMessage.class));
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(Jsons.jsonNode(List.of(state1NoData))));

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state2), AirbyteStateMessage.class));
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(Jsons.jsonNode(List.of(state2NoData, state1NoData))));

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state3), AirbyteStateMessage.class));
    Assertions.assertThat(stateAggregator.getAggregated()).isEqualTo(new State()
        .withState(Jsons.jsonNode(List.of(state3NoData, state1NoData))));
  }

  @Test
  void testIngestFromAnotherStateAggregatorSingleState() {
    final AirbyteStateMessage stateG1 = getGlobalMessage(1);
    stateAggregator.ingest(stateG1);

    final AirbyteStateMessage stateG2 = getGlobalMessage(2);
    final StateAggregator otherStateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    otherStateAggregator.ingest(stateG2);

    stateAggregator.ingest(otherStateAggregator);
    CollectionAssert.assertThatCollection(getStateMessages(stateAggregator.getAggregated()))
        .containsExactlyInAnyOrder(stateG2);
  }

  @Test
  void testIngestFromAnotherStateAggregatorStreamStates() {
    stateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    final AirbyteStateMessage stateA1 = getStreamMessage("a", 1);
    final AirbyteStateMessage stateB2 = getStreamMessage("b", 2);
    stateAggregator.ingest(stateA1);
    stateAggregator.ingest(stateB2);

    final AirbyteStateMessage stateA2 = getStreamMessage("a", 3);
    final AirbyteStateMessage stateC1 = getStreamMessage("c", 1);
    final StateAggregator otherStateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    otherStateAggregator.ingest(stateA2);
    otherStateAggregator.ingest(stateC1);

    stateAggregator.ingest(otherStateAggregator);
    CollectionAssert.assertThatCollection(getStateMessages(stateAggregator.getAggregated()))
        .containsExactlyInAnyOrder(stateA2, stateB2, stateC1);
  }

  @Test
  void testIngestFromAnotherStateAggregatorChecksStateType() {
    stateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    final AirbyteStateMessage stateG1 = getGlobalMessage(1);
    stateAggregator.ingest(stateG1);

    final AirbyteStateMessage stateA2 = getStreamMessage("a", 3);
    final StateAggregator otherStateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    otherStateAggregator.ingest(stateA2);

    Assertions.assertThatThrownBy(() -> {
      stateAggregator.ingest(otherStateAggregator);
    });

    Assertions.assertThatThrownBy(() -> {
      otherStateAggregator.ingest(stateAggregator);
    });
  }

  @Test
  void testIsEmptyForSingleStates() {
    stateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    Assertions.assertThat(stateAggregator.isEmpty()).isTrue();

    final AirbyteStateMessage globalState = getGlobalMessage(1);
    stateAggregator.ingest(globalState);
    Assertions.assertThat(stateAggregator.isEmpty()).isFalse();
  }

  @Test
  void testIsEmptyForStreamStates() {
    stateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    Assertions.assertThat(stateAggregator.isEmpty()).isTrue();

    final AirbyteStateMessage streamState = getStreamMessage("woot", 1);
    stateAggregator.ingest(streamState);
    Assertions.assertThat(stateAggregator.isEmpty()).isFalse();
  }

  @Test
  void testIsEmptyWhenIngestFromAggregatorSingle() {
    stateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    Assertions.assertThat(stateAggregator.isEmpty()).isTrue();

    final AirbyteStateMessage globalState = getGlobalMessage(1);
    final StateAggregator otherAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    otherAggregator.ingest(globalState);

    stateAggregator.ingest(otherAggregator);
    Assertions.assertThat(stateAggregator.isEmpty()).isFalse();
  }

  @Test
  void testIsEmptyWhenIngestFromAggregatorStream() {
    stateAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    Assertions.assertThat(stateAggregator.isEmpty()).isTrue();

    final AirbyteStateMessage streamState = getStreamMessage("woot", 1);
    final StateAggregator otherAggregator = new DefaultStateAggregator(USE_STREAM_CAPABLE_STATE);
    otherAggregator.ingest(streamState);

    stateAggregator.ingest(otherAggregator);
    Assertions.assertThat(stateAggregator.isEmpty()).isFalse();
  }

  private AirbyteStateMessage getNullMessage(final int stateValue) {
    return new AirbyteStateMessage().withData(Jsons.jsonNode(stateValue));
  }

  private AirbyteStateMessage getLegacyMessage(final int stateValue) {
    return new AirbyteStateMessage().withType(LEGACY).withData(Jsons.jsonNode(stateValue));
  }

  private AirbyteStateMessage getGlobalMessage(final int stateValue) {
    return new AirbyteStateMessage().withType(GLOBAL)
        .withGlobal(new AirbyteGlobalState()
            .withStreamStates(
                List.of(
                    new AirbyteStreamState()
                        .withStreamDescriptor(
                            new StreamDescriptor()
                                .withName("test"))
                        .withStreamState(Jsons.jsonNode(stateValue)))))
        .withData(Jsons.jsonNode("HelloWorld"));
  }

  private AirbyteStateMessage getStreamMessage(final String streamName, final int stateValue) {
    return new AirbyteStateMessage().withType(STREAM)
        .withStream(
            new AirbyteStreamState()
                .withStreamDescriptor(
                    new StreamDescriptor()
                        .withName(streamName))
                .withStreamState(Jsons.jsonNode(stateValue)))
        .withData(Jsons.jsonNode("Hello"));
  }

  private AirbyteStateMessage getEmptyMessage(final AirbyteStateType stateType) {
    if (stateType == STREAM) {
      return new AirbyteStateMessage()
          .withType(STREAM)
          .withStream(
              new AirbyteStreamState()
                  .withStreamDescriptor(new StreamDescriptor()));
    }

    return new AirbyteStateMessage().withType(stateType);
  }

  private List<AirbyteStateMessage> getStateMessages(final State state) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(state.getState().elements(), Spliterator.ORDERED), false)
        .map(s -> Jsons.object(s, AirbyteStateMessage.class))
        .toList();
  }

}
