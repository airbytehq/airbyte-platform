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
import io.temporal.activity.ActivityOptions
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test suite for the [TemporalActivityStubInterceptor] class.
 */
internal class TemporalActivityStubInterceptorTest {
  @Test
  @Throws(Exception::class)
  fun testExecutionOfValidWorkflowWithActivities() {
    val activityOptions = Mockito.mock<ActivityOptions?>(ActivityOptions::class.java)
    val testActivity = Mockito.mock<TestActivity?>(TestActivity::class.java)

    val activityOptionsBeanIdentifier = Mockito.mock<BeanIdentifier>(BeanIdentifier::class.java)
    val activityOptionsBeanRegistration = Mockito.mock(BeanRegistration::class.java) as BeanRegistration<ActivityOptions>
    Mockito.`when`<String?>(activityOptionsBeanIdentifier.getName()).thenReturn(ACTIVITY_OPTIONS)
    Mockito.`when`<BeanIdentifier?>(activityOptionsBeanRegistration.identifier).thenReturn(activityOptionsBeanIdentifier)
    Mockito.`when`<Any?>(activityOptionsBeanRegistration.bean).thenReturn(activityOptions)

    val interceptor: TemporalActivityStubInterceptor<ValidTestWorkflowImpl> =
      TemporalActivityStubInterceptor(ValidTestWorkflowImpl::class.java, listOf(activityOptionsBeanRegistration))
    interceptor.setActivityStubGenerator(TemporalActivityStubGeneratorFunction { c: Class<*>?, a: ActivityOptions? -> testActivity })

    val validTestWorklowImpl = ValidTestWorkflowImpl()
    val callable: Callable<Any?> =
      Callable {
        validTestWorklowImpl.run()
        null
      }

    interceptor.execute(validTestWorklowImpl, callable)
    Assertions.assertTrue(validTestWorklowImpl.isHasRun)
  }

  @Test
  fun testExecutionOfValidWorkflowWithActivitiesThatThrows() {
    val activityOptions = Mockito.mock<ActivityOptions?>(ActivityOptions::class.java)
    val testActivity = Mockito.mock<TestActivity?>(TestActivity::class.java)

    val activityOptionsBeanIdentifier = Mockito.mock<BeanIdentifier>(BeanIdentifier::class.java)
    val activityOptionsBeanRegistration = Mockito.mock(BeanRegistration::class.java) as BeanRegistration<ActivityOptions>
    Mockito.`when`<String?>(activityOptionsBeanIdentifier.getName()).thenReturn(ACTIVITY_OPTIONS)
    Mockito.`when`<BeanIdentifier?>(activityOptionsBeanRegistration.identifier).thenReturn(activityOptionsBeanIdentifier)
    Mockito.`when`<Any?>(activityOptionsBeanRegistration.bean).thenReturn(activityOptions)

    val interceptor: TemporalActivityStubInterceptor<ErrorTestWorkflowImpl> =
      TemporalActivityStubInterceptor(ErrorTestWorkflowImpl::class.java, listOf(activityOptionsBeanRegistration))
    interceptor.setActivityStubGenerator(TemporalActivityStubGeneratorFunction { c: Class<*>?, a: ActivityOptions? -> testActivity })

    val errorTestWorkflowImpl = ErrorTestWorkflowImpl()
    val callable: Callable<Any?> =
      Callable {
        errorTestWorkflowImpl.run()
        null
      }

    Assertions.assertThrows<RetryableException?>(
      RetryableException::class.java,
      Executable {
        interceptor.execute(errorTestWorkflowImpl, callable)
      },
    )
  }

  @Test
  @Throws(Exception::class)
  fun testActivityStubsAreOnlyInitializedOnce() {
    val activityStubInitializationCounter = AtomicInteger(0)
    val activityOptions = Mockito.mock<ActivityOptions?>(ActivityOptions::class.java)
    val testActivity = Mockito.mock<TestActivity?>(TestActivity::class.java)
    val activityStubFunction: TemporalActivityStubGeneratorFunction<Class<*>, ActivityOptions, Any> =
      TemporalActivityStubGeneratorFunction { c: Class<*>, a: ActivityOptions ->
        activityStubInitializationCounter.incrementAndGet()
        testActivity
      }

    val activityOptionsBeanIdentifier = Mockito.mock<BeanIdentifier>(BeanIdentifier::class.java)
    val activityOptionsBeanRegistration = Mockito.mock(BeanRegistration::class.java) as BeanRegistration<ActivityOptions>
    Mockito.`when`<String?>(activityOptionsBeanIdentifier.getName()).thenReturn(ACTIVITY_OPTIONS)
    Mockito.`when`<BeanIdentifier?>(activityOptionsBeanRegistration.identifier).thenReturn(activityOptionsBeanIdentifier)
    Mockito.`when`<Any?>(activityOptionsBeanRegistration.bean).thenReturn(activityOptions)

    val interceptor: TemporalActivityStubInterceptor<ValidTestWorkflowImpl> =
      TemporalActivityStubInterceptor(ValidTestWorkflowImpl::class.java, listOf(activityOptionsBeanRegistration))
    interceptor.setActivityStubGenerator(activityStubFunction)

    val validTestWorklowImpl = ValidTestWorkflowImpl()
    val callable: Callable<Any?> =
      Callable {
        validTestWorklowImpl.run()
        null
      }
    interceptor.execute(validTestWorklowImpl, callable)
    interceptor.execute(validTestWorklowImpl, callable)
    interceptor.execute(validTestWorklowImpl, callable)
    interceptor.execute(validTestWorklowImpl, callable)

    Assertions.assertEquals(1, activityStubInitializationCounter.get())
  }

  @Test
  fun testExecutionOfInvalidWorkflowWithActivityWithMissingActivityOptions() {
    val activityOptions = Mockito.mock<ActivityOptions?>(ActivityOptions::class.java)
    val testActivity = Mockito.mock<TestActivity?>(TestActivity::class.java)

    val activityOptionsBeanIdentifier = Mockito.mock<BeanIdentifier>(BeanIdentifier::class.java)
    val activityOptionsBeanRegistration = Mockito.mock(BeanRegistration::class.java) as BeanRegistration<ActivityOptions>
    Mockito.`when`<String?>(activityOptionsBeanIdentifier.getName()).thenReturn(ACTIVITY_OPTIONS)
    Mockito.`when`<BeanIdentifier?>(activityOptionsBeanRegistration.identifier).thenReturn(activityOptionsBeanIdentifier)
    Mockito.`when`<Any?>(activityOptionsBeanRegistration.bean).thenReturn(activityOptions)

    val interceptor: TemporalActivityStubInterceptor<InvalidTestWorkflowImpl> =
      TemporalActivityStubInterceptor(InvalidTestWorkflowImpl::class.java, listOf(activityOptionsBeanRegistration))
    interceptor.setActivityStubGenerator(TemporalActivityStubGeneratorFunction { c: Class<*>?, a: ActivityOptions? -> testActivity })

    val invalidTestWorklowImpl = InvalidTestWorkflowImpl()
    val callable: Callable<Any?> =
      Callable {
        invalidTestWorklowImpl.run()
        null
      }

    val exception =
      Assertions.assertThrows<RuntimeException>(
        RuntimeException::class.java,
        Executable {
          interceptor.execute(invalidTestWorklowImpl, callable)
        },
      )
    Assertions.assertEquals(IllegalStateException::class.java, exception.cause!!.javaClass)
  }

  companion object {
    private const val ACTIVITY_OPTIONS = "activityOptions"
  }
}
