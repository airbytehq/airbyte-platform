/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobRetryStateRequestBody;
import io.airbyte.api.model.generated.RetryStateRead;
import io.airbyte.commons.json.Jsons;
import io.airbyte.server.handlers.RetryStatesHandler;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
@MicronautTest
@Requires(property = "mockito.test.enabled",
          defaultValue = StringUtils.TRUE,
          value = StringUtils.TRUE)
@Requires(env = {Environment.TEST})
class JobRetryStatesApiControllerTest extends BaseControllerTest {

  @Mock
  RetryStatesHandler handler = Mockito.mock(RetryStatesHandler.class);

  @MockBean(RetryStatesHandler.class)
  @Replaces(RetryStatesHandler.class)
  RetryStatesHandler mmStreamStatusesHandler() {
    return handler;
  }

  static String PATH_BASE = "/api/v1/jobs/retry_states";
  static String PATH_GET = PATH_BASE + "/get";
  static String PATH_PUT = PATH_BASE + "/create_or_update";

  @Test
  void getForJobFound() throws Exception {
    when(handler.getByJobId(Mockito.any()))
        .thenReturn(Optional.of(new RetryStateRead()));

    testEndpointStatus(
        HttpRequest.POST(
            PATH_GET,
            Jsons.serialize(Fixtures.jobIdReq())),
        HttpStatus.OK);
  }

  @Test
  void getForJobNotFound() throws Exception {
    when(handler.getByJobId(Mockito.any()))
        .thenReturn(Optional.empty());

    testErrorEndpointStatus(
        HttpRequest.POST(
            PATH_GET,
            Jsons.serialize(Fixtures.jobIdReq())),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void putForJob() throws Exception {
    testEndpointStatus(
        HttpRequest.POST(
            PATH_PUT,
            Jsons.serialize(Fixtures.retryPutReq())),
        HttpStatus.NO_CONTENT);
  }

  static class Fixtures {

    static long jobId1 = 21891253;

    static JobIdRequestBody jobIdReq() {
      return new JobIdRequestBody().id(jobId1);
    }

    static JobRetryStateRequestBody retryPutReq() {
      return new JobRetryStateRequestBody()
          .id(UUID.randomUUID())
          .connectionId(UUID.randomUUID())
          .jobId(jobId1)
          .successiveCompleteFailures(8)
          .totalCompleteFailures(12)
          .successivePartialFailures(4)
          .totalPartialFailures(42);
    }

  }

}
