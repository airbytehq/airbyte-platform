/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateStats;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.models.StateWithId;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

class StateWithIdTest {

  @ParameterizedTest
  @ValueSource(strings = {
    "{\"{\\\"schema\\\":null,\\\"payload\\\":[\\\"db_jagkjrgxhw\\\",{\\\"server\\\":\\\"db_jagkjrgxhw\\\"}]}\":"
        + "\"{\\\"last_snapshot_record\\\":true,\\\"lsn\\\":23896935,\\\"txId\\\":505,\\\"ts_usec\\\":1659422332985000,\\\"snapshot\\\":true}\"}",
    "{\"[\\\"db_jagkjrgxhw\\\",{\\\"server\\\":\\\"db_jagkjrgxhw\\\"}]}\":\"{\\\"last_snapshot_record\\\":true,"
        + "\\\"lsn\\\":23896935,\\\"txId\\\":505,\\\"ts_usec\\\":1659422332985000,\\\"snapshot\\\":true}\"}",
    "{\"[\\\"db_jagkjrgxhw\\\",{\\\"server\\\":\\\"db_jagkjrgxhw\\\"}]\":\"{\\\"transaction_id\\\":null,\\\"lsn\\\":"
        + "23896935,\\\"txId\\\":505,\\\"ts_usec\\\":1677520006097984}\"}"
  })
  void globalStateTest(final String cdcState) {
    final Random random = new Random();
    final double recordCount = random.nextDouble();
    final String cursorName = UUID.randomUUID().toString();

    final AirbyteStateMessage originalState = getAirbyteGlobalStateMessage(cdcState, recordCount, cursorName);
    final AirbyteMessage originalMessage = new AirbyteMessage().withType(AirbyteMessage.Type.STATE)
        .withState(originalState);

    final AirbyteStateMessage copyOfOriginalState = getAirbyteGlobalStateMessage(cdcState, recordCount, cursorName);
    final AirbyteMessage copyOfOriginal = new AirbyteMessage().withType(AirbyteMessage.Type.STATE)
        .withState(copyOfOriginalState);

    assertEquals(originalMessage, copyOfOriginal);
    final AirbyteMessage stateMessageWithIdAdded = StateWithId.attachIdToStateMessageFromSource(copyOfOriginal);
    assertNotEquals(originalMessage, stateMessageWithIdAdded);
    assertEquals(originalMessage.getState().getGlobal(), stateMessageWithIdAdded.getState().getGlobal());

    final String serializedMessage = Jsons.serialize(stateMessageWithIdAdded);
    Optional<AirbyteMessage> deserializedMessage = Jsons.tryDeserializeExact(serializedMessage, AirbyteMessage.class);
    assertEquals(stateMessageWithIdAdded, deserializedMessage.orElseThrow());
    assertEquals(originalMessage.getState().getGlobal(), deserializedMessage.orElseThrow().getState().getGlobal());
    assertEquals(StateWithId.getIdFromStateMessage(stateMessageWithIdAdded).orElseThrow(),
        StateWithId.getIdFromStateMessage(deserializedMessage.orElseThrow()).orElseThrow());
  }

  @Test
  void streamStateTest() {
    final Random random = new Random();
    final double recordCount = random.nextDouble();
    final String cursorName = UUID.randomUUID().toString();

    final AirbyteStateMessage originalState = getAirbyteStreamStateMessage(recordCount, cursorName);
    final AirbyteMessage originalMessage = new AirbyteMessage().withType(AirbyteMessage.Type.STATE)
        .withState(originalState);

    final AirbyteStateMessage copyOfOriginalState = getAirbyteStreamStateMessage(recordCount, cursorName);
    final AirbyteMessage copyOfOriginal = new AirbyteMessage().withType(AirbyteMessage.Type.STATE)
        .withState(copyOfOriginalState);

    assertEquals(originalMessage, copyOfOriginal);
    final AirbyteMessage stateMessageWithIdAdded = StateWithId.attachIdToStateMessageFromSource(copyOfOriginal);
    assertNotEquals(originalMessage, stateMessageWithIdAdded);
    assertEquals(originalMessage.getState().getGlobal(), stateMessageWithIdAdded.getState().getGlobal());

    final String serializedMessage = Jsons.serialize(stateMessageWithIdAdded);
    Optional<AirbyteMessage> deserializedMessage = Jsons.tryDeserializeExact(serializedMessage, AirbyteMessage.class);
    assertEquals(stateMessageWithIdAdded, deserializedMessage.orElseThrow());
    assertEquals(originalMessage.getState().getStream(), deserializedMessage.orElseThrow().getState().getStream());
    assertEquals(StateWithId.getIdFromStateMessage(stateMessageWithIdAdded).orElseThrow(),
        StateWithId.getIdFromStateMessage(deserializedMessage.orElseThrow()).orElseThrow());
  }

  private static AirbyteStateMessage getAirbyteStreamStateMessage(final double recordCount, final String cursorName) {
    final AirbyteStreamState streamState = new AirbyteStreamState()
        .withStreamState(Jsons.jsonNode(ImmutableMap.of(cursorName, 1)))
        .withStreamDescriptor(new StreamDescriptor().withName(cursorName).withNamespace(cursorName));
    final AirbyteStateStats airbyteStateStats = new AirbyteStateStats().withRecordCount(recordCount);
    return new AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
        .withStream(streamState)
        .withSourceStats(airbyteStateStats);
  }

  private static AirbyteStateMessage getAirbyteGlobalStateMessage(final String cdcState, final double recordCount, final String cursorName) {
    final JsonNode cdcStateAsJson = Jsons.deserialize(cdcState);
    final AirbyteGlobalState globalState = new AirbyteGlobalState().withSharedState(cdcStateAsJson).withStreamStates(Collections.singletonList(
        new AirbyteStreamState()
            .withStreamState(Jsons.jsonNode(ImmutableMap.of(cursorName, 1)))
            .withStreamDescriptor(new StreamDescriptor().withName(cursorName).withNamespace(cursorName))));
    final AirbyteStateStats airbyteStateStats = new AirbyteStateStats().withRecordCount(recordCount);
    return new AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
        .withGlobal(globalState)
        .withSourceStats(airbyteStateStats);
  }

}
