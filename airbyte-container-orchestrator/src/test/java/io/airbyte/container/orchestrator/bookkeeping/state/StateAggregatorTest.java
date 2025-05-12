/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.state;

import static io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType.GLOBAL;
import static io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType.LEGACY;
import static io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType.STREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.State;
import io.airbyte.protocol.models.v0.AirbyteGlobalState;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.v0.AirbyteStreamState;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class StateAggregatorTest {

  StateAggregator stateAggregator;

  @BeforeEach
  void init() {
    stateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
  }

  @ParameterizedTest
  @EnumSource(AirbyteStateType.class)
  void testCantMixType(final AirbyteStateType stateType) {
    final Stream<AirbyteStateType> allTypes = Arrays.stream(AirbyteStateType.values());

    stateAggregator.ingest(getEmptyMessage(stateType));

    final List<AirbyteStateType> differentTypes = allTypes.filter(type -> type != stateType).toList();
    differentTypes
        .forEach(differentType -> assertThrows(IllegalArgumentException.class, () -> stateAggregator.ingest(getEmptyMessage(differentType))));
  }

  @Test
  void testCantMixNullType() {
    final List<AirbyteStateType> allIncompatibleTypes = Lists.newArrayList(GLOBAL, STREAM);

    stateAggregator.ingest(getEmptyMessage(null));

    allIncompatibleTypes
        .forEach(differentType -> assertThrows(IllegalArgumentException.class, () -> stateAggregator.ingest(getEmptyMessage(differentType))));

    stateAggregator.ingest(getEmptyMessage(LEGACY));
  }

  @Test
  void testNullState() {
    final AirbyteStateMessage state1 = getNullMessage(1);
    final AirbyteStateMessage state2 = getNullMessage(2);

    stateAggregator.ingest(state1);
    assertEquals(new State().withState(state1.getData()), stateAggregator.getAggregated());

    stateAggregator.ingest(state2);
    assertEquals(new State().withState(state2.getData()), stateAggregator.getAggregated());
  }

  @Test
  void testLegacyState() {
    final AirbyteStateMessage state1 = getLegacyMessage(1);
    final AirbyteStateMessage state2 = getLegacyMessage(2);

    stateAggregator.ingest(state1);
    assertEquals(new State().withState(state1.getData()), stateAggregator.getAggregated());

    stateAggregator.ingest(state2);
    assertEquals(new State().withState(state2.getData()), stateAggregator.getAggregated());
  }

  @Test
  void testGlobalState() {
    final AirbyteStateMessage state1 = getGlobalMessage(1);
    final AirbyteStateMessage state2 = getGlobalMessage(2);

    final AirbyteStateMessage state1NoData = getGlobalMessage(1).withData(null);
    final AirbyteStateMessage state2NoData = getGlobalMessage(2).withData(null);

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state1), AirbyteStateMessage.class));
    assertEquals(new State()
        .withState(Jsons.jsonNode(List.of(state1NoData))), stateAggregator.getAggregated());

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state2), AirbyteStateMessage.class));
    assertEquals(new State()
        .withState(Jsons.jsonNode(List.of(state2NoData))), stateAggregator.getAggregated());
  }

  @Test
  void testStreamState() {
    final AirbyteStateMessage state1 = getStreamMessage("a", 1);
    final AirbyteStateMessage state2 = getStreamMessage("b", 2);
    final AirbyteStateMessage state3 = getStreamMessage("b", 3);

    final AirbyteStateMessage state1NoData = getStreamMessage("a", 1).withData(null);
    final AirbyteStateMessage state2NoData = getStreamMessage("b", 2).withData(null);
    final AirbyteStateMessage state3NoData = getStreamMessage("b", 3).withData(null);

    stateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state1), AirbyteStateMessage.class));
    assertEquals(new State()
        .withState(Jsons.jsonNode(List.of(state1NoData))), stateAggregator.getAggregated());

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state2), AirbyteStateMessage.class));
    assertEquals(new State()
        .withState(Jsons.jsonNode(List.of(state1NoData, state2NoData))), stateAggregator.getAggregated());

    stateAggregator.ingest(Jsons.object(Jsons.jsonNode(state3), AirbyteStateMessage.class));
    assertEquals(new State()
        .withState(Jsons.jsonNode(List.of(state1NoData, state3NoData))), stateAggregator.getAggregated());
  }

  @Test
  void testIngestFromAnotherStateAggregatorSingleState() {
    final AirbyteStateMessage stateG1 = getGlobalMessage(1);
    stateAggregator.ingest(stateG1);

    final AirbyteStateMessage stateG2 = getGlobalMessage(2);
    final StateAggregator otherStateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    otherStateAggregator.ingest(stateG2);

    stateAggregator.ingest(otherStateAggregator);
    assertEquals(List.of(stateG2), getStateMessages(stateAggregator.getAggregated()));
  }

  @Test
  void testIngestFromAnotherStateAggregatorStreamStates() {
    stateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    final AirbyteStateMessage stateA1 = getStreamMessage("a", 1);
    final AirbyteStateMessage stateB2 = getStreamMessage("b", 2);
    stateAggregator.ingest(stateA1);
    stateAggregator.ingest(stateB2);

    final AirbyteStateMessage stateA2 = getStreamMessage("a", 3);
    final AirbyteStateMessage stateC1 = getStreamMessage("c", 1);
    final StateAggregator otherStateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    otherStateAggregator.ingest(stateA2);
    otherStateAggregator.ingest(stateC1);

    stateAggregator.ingest(otherStateAggregator);
    assertEquals(List.of(stateA2, stateB2, stateC1), getStateMessages(stateAggregator.getAggregated()));
  }

  @Test
  void testIngestFromAnotherStateAggregatorChecksStateType() {
    stateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    final AirbyteStateMessage stateG1 = getGlobalMessage(1);
    stateAggregator.ingest(stateG1);

    final AirbyteStateMessage stateA2 = getStreamMessage("a", 3);
    final StateAggregator otherStateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    otherStateAggregator.ingest(stateA2);

    assertThrows(IllegalArgumentException.class, () -> stateAggregator.ingest(otherStateAggregator));
    assertThrows(IllegalArgumentException.class, () -> otherStateAggregator.ingest(stateAggregator));
  }

  @Test
  void testIsEmptyForSingleStates() {
    stateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    assertTrue(stateAggregator.isEmpty());

    final AirbyteStateMessage globalState = getGlobalMessage(1);
    stateAggregator.ingest(globalState);
    assertFalse(stateAggregator.isEmpty());
  }

  @Test
  void testIsEmptyForStreamStates() {
    stateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    assertTrue(stateAggregator.isEmpty());

    final AirbyteStateMessage streamState = getStreamMessage("woot", 1);
    stateAggregator.ingest(streamState);
    assertFalse(stateAggregator.isEmpty());
  }

  @Test
  void testIsEmptyWhenIngestFromAggregatorSingle() {
    stateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    assertTrue(stateAggregator.isEmpty());

    final AirbyteStateMessage globalState = getGlobalMessage(1);
    final StateAggregator otherAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    otherAggregator.ingest(globalState);

    stateAggregator.ingest(otherAggregator);
    assertFalse(stateAggregator.isEmpty());
  }

  @Test
  void testIsEmptyWhenIngestFromAggregatorStream() {
    stateAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    assertTrue(stateAggregator.isEmpty());

    final AirbyteStateMessage streamState = getStreamMessage("woot", 1);
    final StateAggregator otherAggregator = new DefaultStateAggregator(
        new StreamStateAggregator(),
        new SingleStateAggregator());
    otherAggregator.ingest(streamState);

    stateAggregator.ingest(otherAggregator);
    assertFalse(stateAggregator.isEmpty());
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
