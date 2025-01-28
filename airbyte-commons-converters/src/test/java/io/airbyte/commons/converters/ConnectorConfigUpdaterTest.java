/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.model.generated.DestinationIdRequestBody;
import io.airbyte.api.client.model.generated.DestinationRead;
import io.airbyte.api.client.model.generated.DestinationUpdate;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.SourceUpdate;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.Config;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ConnectorConfigUpdaterTest {

  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final String SOURCE_NAME = "source-stripe";
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final String DESTINATION_NAME = "destination-google-sheets";

  private final AirbyteApiClient mAirbyteApiClient = mock(AirbyteApiClient.class);
  private final SourceApi mSourceApi = mock(SourceApi.class);
  private final DestinationApi mDestinationApi = mock(DestinationApi.class);

  private ConnectorConfigUpdater connectorConfigUpdater;

  @BeforeEach
  void setUp() throws IOException {
    when(mSourceApi.getSource(new SourceIdRequestBody(SOURCE_ID))).thenReturn(new SourceRead(
        UUID.randomUUID(),
        SOURCE_ID,
        UUID.randomUUID(),
        Jsons.jsonNode(Map.of()),
        SOURCE_NAME,
        SOURCE_NAME,
        1L,
        null, null, null, null, null));

    when(mDestinationApi.getDestination(new DestinationIdRequestBody(DESTINATION_ID)))
        .thenReturn(new DestinationRead(
            UUID.randomUUID(),
            DESTINATION_ID,
            UUID.randomUUID(),
            Jsons.jsonNode(Map.of()),
            DESTINATION_NAME,
            DESTINATION_NAME,
            1L, null, null, null, null, null));

    when(mAirbyteApiClient.getDestinationApi()).thenReturn(mDestinationApi);
    when(mAirbyteApiClient.getSourceApi()).thenReturn(mSourceApi);

    connectorConfigUpdater = new ConnectorConfigUpdater(mAirbyteApiClient);
  }

  @Test
  void testPersistSourceConfig() throws IOException {
    final Config newConfiguration = new Config().withAdditionalProperty("key", "new_value");
    final JsonNode configJson = Jsons.jsonNode(newConfiguration.getAdditionalProperties());

    final SourceUpdate expectedSourceUpdate = new SourceUpdate(SOURCE_ID, configJson, SOURCE_NAME, null);

    when(mSourceApi.updateSource(Mockito.any())).thenReturn(new SourceRead(
        UUID.randomUUID(),
        SOURCE_ID,
        UUID.randomUUID(),
        configJson,
        SOURCE_NAME,
        SOURCE_NAME,
        1L, null, null, null, null, null));

    connectorConfigUpdater.updateSource(SOURCE_ID, newConfiguration);
    verify(mSourceApi).updateSource(expectedSourceUpdate);
  }

  @Test
  void testPersistDestinationConfig() throws IOException {
    final Config newConfiguration = new Config().withAdditionalProperty("key", "new_value");
    final JsonNode configJson = Jsons.jsonNode(newConfiguration.getAdditionalProperties());

    final DestinationUpdate expectedDestinationUpdate = new DestinationUpdate(DESTINATION_ID, configJson, DESTINATION_NAME);
    final DestinationRead destinationRead = new DestinationRead(
        UUID.randomUUID(),
        DESTINATION_ID,
        UUID.randomUUID(),
        configJson,
        DESTINATION_NAME,
        DESTINATION_NAME,
        1L, null, null, null, null, null);

    when(mDestinationApi.getDestination(new DestinationIdRequestBody(DESTINATION_ID)))
        .thenReturn(destinationRead);
    when(mDestinationApi.updateDestination(expectedDestinationUpdate)).thenReturn(destinationRead);

    connectorConfigUpdater.updateDestination(DESTINATION_ID, newConfiguration);
    verify(mDestinationApi).updateDestination(expectedDestinationUpdate);
  }

}
