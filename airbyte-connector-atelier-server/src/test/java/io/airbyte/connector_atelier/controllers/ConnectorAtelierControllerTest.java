/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_atelier.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.connector_atelier.api.model.generated.ResolveManifest;
import io.airbyte.connector_atelier.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_atelier.api.model.generated.StreamRead;
import io.airbyte.connector_atelier.api.model.generated.StreamReadPages;
import io.airbyte.connector_atelier.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_atelier.api.model.generated.StreamReadSlices;
import io.airbyte.connector_atelier.api.model.generated.StreamsListRead;
import io.airbyte.connector_atelier.api.model.generated.StreamsListRequestBody;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorAtelierControllerTest {

  @Test
  void testGetManifestTemplate() {
    final ConnectorAtelierController controller = new ConnectorAtelierController();

    final String manifestTemplate = controller.getManifestTemplate();
    assertTrue(manifestTemplate.contains("manifest"));
  }

  @Test
  void testListStreams() {
    final ConnectorAtelierController controller = new ConnectorAtelierController();

    final StreamsListRead streams = controller.listStreams(new StreamsListRequestBody());

    assertEquals(1, streams.getStreams().size());
  }

  @Test
  void testReadStream() {
    final ConnectorAtelierController controller = new ConnectorAtelierController();

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
    final ConnectorAtelierController controller = new ConnectorAtelierController();

    final ResolveManifest resolveManifest = controller.resolveManifest(new ResolveManifestRequestBody());

    assertNotNull(resolveManifest.getManifest());
  }

}
