/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobRetryStateRequestBody;
import io.airbyte.api.model.generated.RetryStateRead;
import io.airbyte.server.handlers.api_domain_mapping.RetryStatesMapper;
import io.airbyte.server.repositories.RetryStatesRepository;
import io.airbyte.server.repositories.domain.RetryState;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

class RetryStatesHandlerTest {

  @Mock
  RetryStatesRepository repo;

  @Mock
  RetryStatesMapper mapper;

  RetryStatesHandler handler;

  @BeforeEach
  void setup() {
    repo = Mockito.mock(RetryStatesRepository.class);
    mapper = Mockito.mock(RetryStatesMapper.class);
    handler = new RetryStatesHandler(repo, mapper);
  }

  @Test
  void testGetByJobId() {
    final var jobId = 1345L;
    final var apiReq = new JobIdRequestBody().id(jobId);
    final var domain = new RetryState.RetryStateBuilder().build();
    final var apiResp = new RetryStateRead();

    when(repo.findByJobId(jobId))
        .thenReturn(Optional.of(domain));
    when(mapper.map(domain))
        .thenReturn(apiResp);

    final var result = handler.getByJobId(apiReq);

    Assertions.assertTrue(result.isPresent());
    Assertions.assertSame(apiResp, result.get());
  }

  @Test
  void testPutByJobIdUpdate() {
    final var jobId = 1345L;
    final var apiReq = new JobRetryStateRequestBody()
        .jobId(jobId);

    final var update = new RetryState.RetryStateBuilder()
        .jobId(jobId)
        .build();

    when(mapper.map(apiReq))
        .thenReturn(update);

    handler.putByJobId(apiReq);

    verify(repo, times(1)).createOrUpdateByJobId(jobId, update);
  }

}
