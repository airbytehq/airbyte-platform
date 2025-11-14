/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal

import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.micronaut.temporal.stubs.ErrorTestWorkflowImpl
import io.airbyte.micronaut.temporal.stubs.InvalidTestWorkflowImpl
import io.airbyte.micronaut.temporal.stubs.TestActivity
import io.airbyte.micronaut.temporal.stubs.ValidTestWorkflowImpl
import io.micronaut.context.BeanRegistration
import io.micronaut.inject.BeanIdentifier
import io.mockk.every
import io.mockk.mockk
import io.temporal.activity.ActivityOptions
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test suite for the [TemporalActivityStubInterceptor] class.
 */
internal class TemporalActivityStubInterceptorTest {
  @Suppress("UNCHECKED_CAST")
  @Test
  fun testExecutionOfValidWorkflowWithActivities() {
    val activityOptions = mockk<ActivityOptions>()
    val testActivity = mockk<TestActivity>(relaxed = true)

    val activityOptionsBeanIdentifier = mockk<BeanIdentifier>()
    val activityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { activityOptionsBeanIdentifier.name } returns ACTIVITY_OPTIONS
    every { activityOptionsBeanRegistration.identifier } returns activityOptionsBeanIdentifier
    every { activityOptionsBeanRegistration.bean } returns activityOptions

    val interceptor: TemporalActivityStubInterceptor<ValidTestWorkflowImpl> =
      TemporalActivityStubInterceptor(ValidTestWorkflowImpl::class.java, listOf(activityOptionsBeanRegistration))
    interceptor.setActivityStubGenerator { _: Class<*>?, _: ActivityOptions? -> testActivity }

    val validTestWorkflowImpl = ValidTestWorkflowImpl()
    val callable: Callable<Any?> =
      Callable {
        validTestWorkflowImpl.run()
        null
      }

    interceptor.execute(validTestWorkflowImpl, callable)
    Assertions.assertTrue(validTestWorkflowImpl.isHasRun)
  }

  @Test
  fun testExecutionOfValidWorkflowWithActivitiesThatThrows() {
    val activityOptions = mockk<ActivityOptions>()
    val testActivity = mockk<TestActivity>(relaxed = true)

    val activityOptionsBeanIdentifier = mockk<BeanIdentifier>()
    val activityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { activityOptionsBeanIdentifier.name } returns ACTIVITY_OPTIONS
    every { activityOptionsBeanRegistration.identifier } returns activityOptionsBeanIdentifier
    every { activityOptionsBeanRegistration.bean } returns activityOptions

    val interceptor: TemporalActivityStubInterceptor<ErrorTestWorkflowImpl> =
      TemporalActivityStubInterceptor(ErrorTestWorkflowImpl::class.java, listOf(activityOptionsBeanRegistration))
    interceptor.setActivityStubGenerator { _: Class<*>?, _: ActivityOptions? -> testActivity }

    val errorTestWorkflowImpl = ErrorTestWorkflowImpl()
    val callable: Callable<Any?> =
      Callable {
        errorTestWorkflowImpl.run()
        null
      }

    Assertions.assertThrows(
      RetryableException::class.java,
    ) {
      interceptor.execute(errorTestWorkflowImpl, callable)
    }
  }

  @Test
  fun testActivityStubsAreOnlyInitializedOnce() {
    val activityStubInitializationCounter = AtomicInteger(0)
    val activityOptions = mockk<ActivityOptions>()
    val testActivity = mockk<TestActivity>(relaxed = true)
    val activityStubFunction: TemporalActivityStubGeneratorFunction<Class<*>, ActivityOptions, Any> =
      TemporalActivityStubGeneratorFunction { _: Class<*>, _: ActivityOptions ->
        activityStubInitializationCounter.incrementAndGet()
        testActivity
      }

    val activityOptionsBeanIdentifier = mockk<BeanIdentifier>()
    val activityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { activityOptionsBeanIdentifier.name } returns ACTIVITY_OPTIONS
    every { activityOptionsBeanRegistration.identifier } returns activityOptionsBeanIdentifier
    every { activityOptionsBeanRegistration.bean } returns activityOptions

    val interceptor: TemporalActivityStubInterceptor<ValidTestWorkflowImpl> =
      TemporalActivityStubInterceptor(ValidTestWorkflowImpl::class.java, listOf(activityOptionsBeanRegistration))
    interceptor.setActivityStubGenerator(activityStubFunction)

    val validTestWorkflowImpl = ValidTestWorkflowImpl()
    val callable: Callable<Any?> =
      Callable {
        validTestWorkflowImpl.run()
        null
      }
    interceptor.execute(validTestWorkflowImpl, callable)
    interceptor.execute(validTestWorkflowImpl, callable)
    interceptor.execute(validTestWorkflowImpl, callable)
    interceptor.execute(validTestWorkflowImpl, callable)

    Assertions.assertEquals(1, activityStubInitializationCounter.get())
  }

  @Test
  fun testExecutionOfInvalidWorkflowWithActivityWithMissingActivityOptions() {
    val activityOptions = mockk<ActivityOptions>()
    val testActivity = mockk<TestActivity>(relaxed = true)

    val activityOptionsBeanIdentifier = mockk<BeanIdentifier>()
    val activityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { activityOptionsBeanIdentifier.name } returns ACTIVITY_OPTIONS
    every { activityOptionsBeanRegistration.identifier } returns activityOptionsBeanIdentifier
    every { activityOptionsBeanRegistration.bean } returns activityOptions

    val interceptor: TemporalActivityStubInterceptor<InvalidTestWorkflowImpl> =
      TemporalActivityStubInterceptor(InvalidTestWorkflowImpl::class.java, listOf(activityOptionsBeanRegistration))
    interceptor.setActivityStubGenerator { _: Class<*>?, _: ActivityOptions? -> testActivity }

    val invalidTestWorkflowImpl = InvalidTestWorkflowImpl()
    val callable: Callable<Any?> =
      Callable {
        invalidTestWorkflowImpl.run()
        null
      }

    val exception =
      Assertions.assertThrows(
        RuntimeException::class.java,
      ) {
        interceptor.execute(invalidTestWorkflowImpl, callable)
      }
    Assertions.assertEquals(IllegalStateException::class.java, exception.cause!!.javaClass)
  }

  companion object {
    private const val ACTIVITY_OPTIONS = "activityOptions"
  }
}
