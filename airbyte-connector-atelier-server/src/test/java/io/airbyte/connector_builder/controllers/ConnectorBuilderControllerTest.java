/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadPages;
import io.airbyte.connector_builder.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlices;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.api.model.generated.StreamsListRequestBody;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorBuilderControllerTest {

  @Test
  void testListStreams() {
    final ConnectorBuilderController controller = new ConnectorBuilderController();

    final StreamsListRead streams = controller.listStreams(new StreamsListRequestBody());

    assertEquals(2, streams.getStreams().size());
  }

  @Test
  void testReadStream() {
    final ConnectorBuilderController controller = new ConnectorBuilderController();

    final StreamRead readResponse = controller.readStream(new StreamReadRequestBody());

    final List<StreamReadSlices> slices = readResponse.getSlices();
    assertEquals(1, slices.size());

    final List<StreamReadPages> pages = slices.get(0).getPages();
    assertEquals(1, pages.size());

    final List<Object> records = pages.get(0).getRecords();
    assertEquals(2, records.size());
  }

  @Test
  void testResolveManifest() {
    final ConnectorBuilderController controller = new ConnectorBuilderController();

    final ResolveManifest resolveManifest = controller.resolveManifest(new ResolveManifestRequestBody());

    assertNotNull(resolveManifest.getManifest());
  }

}
