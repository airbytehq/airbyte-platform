/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.config

import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.DependencyInjectionException
import io.temporal.serviceclient.ServiceStubsOptions
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.time.Duration

/**
 * Test suite for the [TemporalSdkTimeouts] class.
 */
internal class TemporalSdkTimeoutsTest {
  @Test
  fun testTemporalSdkTimeouts() {
    val rpcTimeout = Duration.ofSeconds(25)
    val rpcLongPollTimeout = Duration.ofSeconds(35)
    val rpcQueryTimeout = Duration.ofSeconds(45)
    val timeouts =
      mapOf<String?, Any?>(
        "temporal.sdk.timeouts.rpc-timeout" to rpcTimeout,
        "temporal.sdk.timeouts.rpc-long-poll-timeout" to rpcLongPollTimeout,
        "temporal.sdk.timeouts.rpc-query-timeout" to rpcQueryTimeout,
      )

    val ctx = ApplicationContext.run(timeouts)
    val temporalSdkTimeouts = ctx.getBean(TemporalSdkTimeouts::class.java)

    Assertions.assertEquals(rpcTimeout, temporalSdkTimeouts.rpcTimeout)
    Assertions.assertEquals(rpcLongPollTimeout, temporalSdkTimeouts.rpcLongPollTimeout)
    Assertions.assertEquals(rpcQueryTimeout, temporalSdkTimeouts.rpcQueryTimeout)
  }

  @Test
  fun testTemporalSdkTimeoutsStringValues() {
    val rpcTimeout = Duration.ofSeconds(25)
    val rpcLongPollTimeout = Duration.ofSeconds(35)
    val rpcQueryTimeout = Duration.ofSeconds(45)
    val timeouts =
      mapOf<String?, Any?>(
        "temporal.sdk.timeouts.rpc-timeout" to "25s",
        "temporal.sdk.timeouts.rpc-long-poll-timeout" to "35s",
        "temporal.sdk.timeouts.rpc-query-timeout" to "45s",
      )

    val ctx = ApplicationContext.run(timeouts)
    val temporalSdkTimeouts = ctx.getBean(TemporalSdkTimeouts::class.java)

    Assertions.assertEquals(rpcTimeout, temporalSdkTimeouts.rpcTimeout)
    Assertions.assertEquals(rpcLongPollTimeout, temporalSdkTimeouts.rpcLongPollTimeout)
    Assertions.assertEquals(rpcQueryTimeout, temporalSdkTimeouts.rpcQueryTimeout)
  }

  @Test
  fun testTemporalSdkTimeoutsEmptyValues() {
    val timeouts: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    timeouts.put("temporal.sdk.timeouts.rpc-timeout", null)
    timeouts.put("temporal.sdk.timeouts.rpc-long-poll-timeout", null)
    timeouts.put("temporal.sdk.timeouts.rpc-query-timeout", null)

    val ctx = ApplicationContext.run(timeouts)
    Assertions.assertThrows<DependencyInjectionException>(
      DependencyInjectionException::class.java,
      Executable {
        ctx.getBean(
          TemporalSdkTimeouts::class.java,
        )
      },
    )
  }

  @Test
  fun testTemporalSdkTimeoutsWithDefaults() {
    val timeouts = mapOf<String?, Any?>()

    val ctx = ApplicationContext.run(timeouts)
    val temporalSdkTimeouts = ctx.getBean(TemporalSdkTimeouts::class.java)

    Assertions.assertEquals(ServiceStubsOptions.DEFAULT_RPC_TIMEOUT, temporalSdkTimeouts.rpcTimeout)
    Assertions.assertEquals(WorkflowServiceStubsOptions.DEFAULT_POLL_RPC_TIMEOUT, temporalSdkTimeouts.rpcLongPollTimeout)
    Assertions.assertEquals(WorkflowServiceStubsOptions.DEFAULT_QUERY_RPC_TIMEOUT, temporalSdkTimeouts.rpcQueryTimeout)
  }
}
