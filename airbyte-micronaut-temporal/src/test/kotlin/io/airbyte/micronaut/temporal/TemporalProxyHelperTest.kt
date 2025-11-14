/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal

import io.airbyte.micronaut.temporal.stubs.TestActivity
import io.airbyte.micronaut.temporal.stubs.ValidTestWorkflowImpl
import io.micronaut.context.BeanRegistration
import io.micronaut.inject.BeanIdentifier
import io.mockk.every
import io.mockk.mockk
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Test suite for the [TemporalProxyHelper] class.
 */
internal class TemporalProxyHelperTest {
  @Test
  fun testProxyToImplementation() {
    val activityOptions =
      ActivityOptions
        .newBuilder()
        .setHeartbeatTimeout(Duration.ofSeconds(30))
        .setStartToCloseTimeout(Duration.ofSeconds(120))
        .setRetryOptions(
          RetryOptions
            .newBuilder()
            .setMaximumAttempts(5)
            .setInitialInterval(Duration.ofSeconds(30))
            .setMaximumInterval(Duration.ofSeconds(600))
            .build(),
        ).build()

    val activityOptionsBeanIdentifier = mockk<BeanIdentifier>()
    val activityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { activityOptionsBeanIdentifier.name } returns "activityOptions"
    every { activityOptionsBeanRegistration.identifier } returns activityOptionsBeanIdentifier
    every { activityOptionsBeanRegistration.bean } returns activityOptions

    val temporalProxyHelper = TemporalProxyHelper(listOf(activityOptionsBeanRegistration))
    val testActivity = mockk<TestActivity>(relaxed = true)
    temporalProxyHelper.setActivityStubGenerator { _: Class<*>?, _: ActivityOptions? -> testActivity }

    val proxy = temporalProxyHelper.proxyWorkflowClass(ValidTestWorkflowImpl::class.java)

    Assertions.assertNotNull(proxy)

    val proxyImplementation = proxy.getDeclaredConstructor().newInstance()
    proxyImplementation.run()
    Assertions.assertTrue(proxyImplementation.isHasRun)
  }
}
