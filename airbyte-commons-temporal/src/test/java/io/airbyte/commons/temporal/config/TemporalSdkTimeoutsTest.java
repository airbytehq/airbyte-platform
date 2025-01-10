/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.config;

import static io.temporal.serviceclient.WorkflowServiceStubsOptions.DEFAULT_POLL_RPC_TIMEOUT;
import static io.temporal.serviceclient.WorkflowServiceStubsOptions.DEFAULT_QUERY_RPC_TIMEOUT;
import static io.temporal.serviceclient.WorkflowServiceStubsOptions.DEFAULT_RPC_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.DependencyInjectionException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link TemporalSdkTimeouts} class.
 */
class TemporalSdkTimeoutsTest {

  @Test
  void testTemporalSdkTimeouts() {
    final Duration rpcTimeout = Duration.ofSeconds(25);
    final Duration rpcLongPollTimeout = Duration.ofSeconds(35);
    final Duration rpcQueryTimeout = Duration.ofSeconds(45);
    final Map<String, Object> timeouts = Map.of(
        "temporal.sdk.timeouts.rpc-timeout", rpcTimeout,
        "temporal.sdk.timeouts.rpc-long-poll-timeout", rpcLongPollTimeout,
        "temporal.sdk.timeouts.rpc-query-timeout", rpcQueryTimeout);

    final ApplicationContext ctx = ApplicationContext.run(timeouts);
    final TemporalSdkTimeouts temporalSdkTimeouts = ctx.getBean(TemporalSdkTimeouts.class);

    assertEquals(rpcTimeout, temporalSdkTimeouts.getRpcTimeout());
    assertEquals(rpcLongPollTimeout, temporalSdkTimeouts.getRpcLongPollTimeout());
    assertEquals(rpcQueryTimeout, temporalSdkTimeouts.getRpcQueryTimeout());
  }

  @Test
  void testTemporalSdkTimeoutsStringValues() {
    final Duration rpcTimeout = Duration.ofSeconds(25);
    final Duration rpcLongPollTimeout = Duration.ofSeconds(35);
    final Duration rpcQueryTimeout = Duration.ofSeconds(45);
    final Map<String, Object> timeouts = Map.of(
        "temporal.sdk.timeouts.rpc-timeout", "25s",
        "temporal.sdk.timeouts.rpc-long-poll-timeout", "35s",
        "temporal.sdk.timeouts.rpc-query-timeout", "45s");

    final ApplicationContext ctx = ApplicationContext.run(timeouts);
    final TemporalSdkTimeouts temporalSdkTimeouts = ctx.getBean(TemporalSdkTimeouts.class);

    assertEquals(rpcTimeout, temporalSdkTimeouts.getRpcTimeout());
    assertEquals(rpcLongPollTimeout, temporalSdkTimeouts.getRpcLongPollTimeout());
    assertEquals(rpcQueryTimeout, temporalSdkTimeouts.getRpcQueryTimeout());
  }

  @Test
  void testTemporalSdkTimeoutsEmptyValues() {
    final Map<String, Object> timeouts = new HashMap<>();
    timeouts.put("temporal.sdk.timeouts.rpc-timeout", null);
    timeouts.put("temporal.sdk.timeouts.rpc-long-poll-timeout", null);
    timeouts.put("temporal.sdk.timeouts.rpc-query-timeout", null);

    final ApplicationContext ctx = ApplicationContext.run(timeouts);
    assertThrows(DependencyInjectionException.class, () -> ctx.getBean(TemporalSdkTimeouts.class));
  }

  @Test
  void testTemporalSdkTimeoutsWithDefaults() {
    final Map<String, Object> timeouts = Map.of();

    final ApplicationContext ctx = ApplicationContext.run(timeouts);
    final TemporalSdkTimeouts temporalSdkTimeouts = ctx.getBean(TemporalSdkTimeouts.class);

    assertEquals(DEFAULT_RPC_TIMEOUT, temporalSdkTimeouts.getRpcTimeout());
    assertEquals(DEFAULT_POLL_RPC_TIMEOUT, temporalSdkTimeouts.getRpcLongPollTimeout());
    assertEquals(DEFAULT_QUERY_RPC_TIMEOUT, temporalSdkTimeouts.getRpcQueryTimeout());
  }

}
