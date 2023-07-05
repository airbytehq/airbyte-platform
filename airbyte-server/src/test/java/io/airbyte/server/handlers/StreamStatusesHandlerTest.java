/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusReadList;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.server.handlers.api_domain_mapping.StreamStatusesMapper;
import io.airbyte.server.repositories.StreamStatusesRepository;
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams;
import io.airbyte.server.repositories.domain.StreamStatus;
import io.micronaut.data.model.Page;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("MissingJavadocType")
class StreamStatusesHandlerTest {

  StreamStatusesRepository repo;

  StreamStatusesMapper mapper;

  StreamStatusesHandler handler;

  @BeforeEach
  void setup() {
    repo = Mockito.mock(StreamStatusesRepository.class);
    mapper = Mockito.mock(StreamStatusesMapper.class);
    handler = new StreamStatusesHandler(repo, mapper);
  }

  @Test
  void testCreate() {
    final var apiReq = new StreamStatusCreateRequestBody();
    final var domain = StreamStatus.builder().build();
    final var apiResp = new StreamStatusRead();

    when(mapper.map(apiReq))
        .thenReturn(domain);
    when(repo.save(domain))
        .thenReturn(domain);
    when(mapper.map(domain))
        .thenReturn(apiResp);

    Assertions.assertSame(apiResp, handler.createStreamStatus(apiReq));
  }

  @Test
  void testUpdate() {
    final var apiReq = new StreamStatusUpdateRequestBody();
    final var domain = StreamStatus.builder().build();
    final var apiResp = new StreamStatusRead();

    when(mapper.map(apiReq))
        .thenReturn(domain);
    when(repo.update(domain))
        .thenReturn(domain);
    when(mapper.map(domain))
        .thenReturn(apiResp);

    Assertions.assertSame(apiResp, handler.updateStreamStatus(apiReq));
  }

  @Test
  void testList() {
    final var apiReq = new StreamStatusListRequestBody();
    final var domainFilters = FilterParams.builder().build();
    final var domainItem = StreamStatus.builder().build();
    final var apiItem = new StreamStatusRead();
    final var apiResp = new StreamStatusReadList().streamStatuses(List.of(apiItem));

    when(mapper.map(Mockito.any(StreamStatusListRequestBody.class)))
        .thenReturn(domainFilters);

    final var page = Mockito.mock(Page.class);
    when(page.getContent()).thenReturn(List.of(domainItem));
    when(repo.findAllFiltered(domainFilters))
        .thenReturn(page);
    when(mapper.map(domainItem))
        .thenReturn(apiItem);

    Assertions.assertEquals(apiResp, handler.listStreamStatus(apiReq));
  }

  @Test
  void testListPerRunState() {
    final var connectionId = UUID.randomUUID();
    final var apiReq = new ConnectionIdRequestBody().connectionId(connectionId);
    final var domainItem = StreamStatus.builder().build();
    final var apiItem = new StreamStatusRead();
    final var apiResp = new StreamStatusReadList().streamStatuses(List.of(apiItem));

    when(repo.findAllPerRunStateByConnectionId(connectionId))
        .thenReturn(List.of(domainItem));
    when(mapper.map(domainItem))
        .thenReturn(apiItem);

    Assertions.assertEquals(apiResp, handler.listStreamStatusPerRunState(apiReq));
  }

}
