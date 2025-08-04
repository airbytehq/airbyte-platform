/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal

import io.airbyte.micronaut.temporal.stubs.ValidTestWorkflowImpl
import io.micronaut.context.BeanRegistration
import io.micronaut.inject.BeanIdentifier
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.lang.reflect.InvocationTargetException
import java.time.Duration

/**
 * Test suite for the [TemporalProxyHelper] class.
 */
internal class TemporalProxyHelperTest {
  @Test
  @Throws(NoSuchMethodException::class, InvocationTargetException::class, InstantiationException::class, IllegalAccessException::class)
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

    val activityOptionsBeanIdentifier = Mockito.mock<BeanIdentifier>(BeanIdentifier::class.java)
    val activityOptionsBeanRegistration = Mockito.mock(BeanRegistration::class.java) as BeanRegistration<ActivityOptions>
    Mockito.`when`<String?>(activityOptionsBeanIdentifier.getName()).thenReturn("activityOptions")
    Mockito.`when`<BeanIdentifier?>(activityOptionsBeanRegistration.identifier).thenReturn(activityOptionsBeanIdentifier)
    Mockito.`when`<ActivityOptions?>(activityOptionsBeanRegistration.bean).thenReturn(activityOptions)

    val temporalProxyHelper = TemporalProxyHelper(listOf(activityOptionsBeanRegistration))
    temporalProxyHelper.setActivityStubGenerator(TemporalActivityStubGeneratorFunction { c: Class<*>?, a: ActivityOptions? -> Mockito.mock(c) })

    val proxy = temporalProxyHelper.proxyWorkflowClass<ValidTestWorkflowImpl>(ValidTestWorkflowImpl::class.java)

    Assertions.assertNotNull(proxy)

    val proxyImplementation = proxy.getDeclaredConstructor().newInstance()
    proxyImplementation.run()
    Assertions.assertTrue(proxyImplementation.isHasRun)
  }
}
