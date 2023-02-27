/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_atelier.controllers;

import io.airbyte.connector_atelier.api.generated.V1Api;
import io.airbyte.connector_atelier.api.model.generated.ResolveManifest;
import io.airbyte.connector_atelier.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_atelier.api.model.generated.StreamRead;
import io.airbyte.connector_atelier.api.model.generated.StreamReadPages;
import io.airbyte.connector_atelier.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_atelier.api.model.generated.StreamReadSlices;
import io.airbyte.connector_atelier.api.model.generated.StreamsListRead;
import io.airbyte.connector_atelier.api.model.generated.StreamsListReadStreams;
import io.airbyte.connector_atelier.api.model.generated.StreamsListRequestBody;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import java.util.HashMap;
import java.util.List;

@Controller("/v1")
@Context
public class ConnectorAtelierController implements V1Api {

  public ConnectorAtelierController() {
    // Placeholder for now. We just return dummy responses for the base server right now, but we should
    // define any helper handlers here
  }

  @Override
  @Get(uri = "/manifest_template")
  @ExecuteOn(TaskExecutors.IO)
  public String getManifestTemplate() {
    return "The manifest you are looking for is in another castle -Toad";
  }

  @Override
  @Post(uri = "/streams/list",
        produces = MediaType.APPLICATION_JSON)
  @ExecuteOn(TaskExecutors.IO)
  public StreamsListRead listStreams(final StreamsListRequestBody streamsListRequestBody) {
    final StreamsListReadStreams survivors_stream = new StreamsListReadStreams();
    survivors_stream.setName("survivors");
    survivors_stream.setUrl("https://the-last-of-us.com/v1/survivors");
    final StreamsListReadStreams locations_stream = new StreamsListReadStreams();
    locations_stream.setName("locations");
    locations_stream.setUrl("https://the-last-of-us.com/v1/locations");

    final StreamsListRead streamsResponse = new StreamsListRead();
    streamsResponse.setStreams(List.of(survivors_stream, locations_stream));
    return streamsResponse;
  }

  @Override
  @Post(uri = "/stream/read",
        produces = MediaType.APPLICATION_JSON)
  @ExecuteOn(TaskExecutors.IO)
  public StreamRead readStream(final StreamReadRequestBody streamReadRequestBody) {
    final HashMap<String, String> recordOne = new HashMap<>();
    recordOne.put("name", "Joel Miller");
    final HashMap<String, String> recordTwo = new HashMap<>();
    recordTwo.put("name", "Ellie Williams");

    final StreamReadPages pages = new StreamReadPages();
    pages.setRecords(List.of(recordOne, recordTwo));
    final StreamReadSlices slices = new StreamReadSlices();
    slices.setPages(List.of(pages));
    final StreamRead readResponse = new StreamRead();
    readResponse.setSlices(List.of(slices));
    return readResponse;
  }

  @Override
  @Post(uri = "/manifest/resolve",
        produces = MediaType.APPLICATION_JSON)
  @ExecuteOn(TaskExecutors.IO)
  public ResolveManifest resolveManifest(final ResolveManifestRequestBody resolveManifestRequestBody) {
    final ResolveManifest resolvedManifest = new ResolveManifest();
    resolvedManifest.setManifest("Resolved a manifest");
    return resolvedManifest;
  }

}
