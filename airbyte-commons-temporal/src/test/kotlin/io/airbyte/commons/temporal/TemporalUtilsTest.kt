/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.concurrency.VoidCallable
import io.micrometer.core.instrument.MeterRegistry
import io.temporal.activity.Activity
import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.activity.ActivityOptions
import io.temporal.api.namespace.v1.NamespaceInfo
import io.temporal.api.workflowservice.v1.DescribeNamespaceResponse
import io.temporal.client.WorkflowFailedException
import io.temporal.client.WorkflowOptions
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Optional
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

internal class TemporalUtilsTest {
  @Test
  fun testWaitForTemporalServerAndLogThrowsException() {
    val temporalUtils = TemporalUtils(null, null, false, null, null, null, 10, Optional.empty<MeterRegistry>())
    val workflowServiceStubs = Mockito.mock(WorkflowServiceStubs::class.java, Mockito.RETURNS_DEEP_STUBS)
    val describeNamespaceResponse = Mockito.mock(DescribeNamespaceResponse::class.java)
    val namespaceInfo = Mockito.mock(NamespaceInfo::class.java)
    val serviceSupplier = Mockito.mock(Supplier::class.java) as Supplier<WorkflowServiceStubs>
    val namespace = "default"

    Mockito.`when`(namespaceInfo.isInitialized()).thenReturn(true)
    Mockito.`when`(namespaceInfo.getName()).thenReturn(namespace)
    Mockito.`when`(describeNamespaceResponse.getNamespaceInfo()).thenReturn(namespaceInfo)
    Mockito
      .`when`(serviceSupplier.get())
      .thenThrow(java.lang.RuntimeException::class.java)
      .thenReturn(workflowServiceStubs)
    Mockito
      .`when`(
        workflowServiceStubs.blockingStub().describeNamespace(
          any(),
        ),
      ).thenThrow(java.lang.RuntimeException::class.java)
      .thenReturn(describeNamespaceResponse)
    temporalUtils.getTemporalClientWhenConnected(Duration.ofMillis(10), Duration.ofSeconds(1), Duration.ofSeconds(0), serviceSupplier, namespace)
  }

  @Test
  fun testWaitThatTimesOut() {
    val temporalUtils = TemporalUtils(null, null, false, null, null, null, 10, Optional.empty<MeterRegistry>())
    val workflowServiceStubs = Mockito.mock(WorkflowServiceStubs::class.java, Mockito.RETURNS_DEEP_STUBS)
    val describeNamespaceResponse = Mockito.mock(DescribeNamespaceResponse::class.java)
    val namespaceInfo = Mockito.mock(NamespaceInfo::class.java)
    val serviceSupplier = Mockito.mock(Supplier::class.java) as Supplier<WorkflowServiceStubs>
    val namespace = "default"

    Mockito.`when`(namespaceInfo.getName()).thenReturn(namespace)
    Mockito.`when`(describeNamespaceResponse.getNamespaceInfo()).thenReturn(namespaceInfo)
    Mockito
      .`when`(serviceSupplier.get())
      .thenThrow(java.lang.RuntimeException::class.java)
      .thenReturn(workflowServiceStubs)
    Mockito
      .`when`(
        workflowServiceStubs
          .blockingStub()
          .listNamespaces(
            any(),
          ).getNamespacesList(),
      ).thenThrow(java.lang.RuntimeException::class.java)
      .thenReturn(listOf(describeNamespaceResponse))
    Assertions.assertThrows<java.lang.RuntimeException>(
      java.lang.RuntimeException::class.java,
      Executable {
        temporalUtils.getTemporalClientWhenConnected(
          Duration.ofMillis(100),
          Duration.ofMillis(10),
          Duration.ofSeconds(0),
          serviceSupplier,
          namespace,
        )
      },
    )
  }

  @Test
  fun testRuntimeExceptionOnHeartbeatWrapper() {
    val testEnv = TestWorkflowEnvironment.newInstance()
    val worker = testEnv.newWorker(TASK_QUEUE)
    worker.registerWorkflowImplementationTypes(TestFailingWorkflow.WorkflowImpl::class.java)
    val client = testEnv.getWorkflowClient()
    val timesReachedEnd = AtomicInteger(0)
    worker.registerActivitiesImplementations(TestFailingWorkflow.Activity1Impl(timesReachedEnd))
    testEnv.start()

    val workflowStub: TestFailingWorkflow =
      client.newWorkflowStub<TestFailingWorkflow>(
        TestFailingWorkflow::class.java,
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build(),
      )

    // test runtime first
    Assertions.assertThrows<java.lang.RuntimeException>(
      java.lang.RuntimeException::class.java,
      Executable {
        workflowStub.run("runtime")
      },
    )

    // we should never retry enough to reach the end
    Assertions.assertEquals(0, timesReachedEnd.get())
  }

  @Test
  fun testWorkerExceptionOnHeartbeatWrapper() {
    val testEnv = TestWorkflowEnvironment.newInstance()
    val worker = testEnv.newWorker(TASK_QUEUE)
    worker.registerWorkflowImplementationTypes(TestFailingWorkflow.WorkflowImpl::class.java)
    val client = testEnv.getWorkflowClient()
    val timesReachedEnd = AtomicInteger(0)
    worker.registerActivitiesImplementations(TestFailingWorkflow.Activity1Impl(timesReachedEnd))
    testEnv.start()

    val workflowStub: TestFailingWorkflow =
      client.newWorkflowStub<TestFailingWorkflow>(
        TestFailingWorkflow::class.java,
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build(),
      )

    // throws workerexception wrapped in a WorkflowFailedException
    Assertions.assertThrows<WorkflowFailedException>(WorkflowFailedException::class.java, Executable { workflowStub.run("worker") })

    // we should never retry enough to reach the end
    Assertions.assertEquals(0, timesReachedEnd.get())
  }

  @WorkflowInterface
  interface TestWorkflow {
    @WorkflowMethod
    fun run(arg: String?): String?

    class WorkflowImpl : TestWorkflow {
      private val options: ActivityOptions =
        ActivityOptions
          .newBuilder()
          .setScheduleToCloseTimeout(Duration.ofDays(3))
          .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
          .setRetryOptions(TemporalConstants.NO_RETRY)
          .build()

      private val activity1: Activity1 = Workflow.newActivityStub<Activity1>(Activity1::class.java, options)
      private val activity2: Activity1 = Workflow.newActivityStub<Activity1>(Activity1::class.java, options)

      override fun run(arg: String?): String {
        LOGGER.info("workflow before activity 1")
        activity1.activity()
        LOGGER.info("workflow before activity 2")
        activity2.activity()
        LOGGER.info("workflow after all activities")

        return "completed"
      }

      companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(WorkflowImpl::class.java)
      }
    }

    @ActivityInterface
    interface Activity1 {
      @ActivityMethod
      fun activity()
    }

    class Activity1Impl(
      private val callable: VoidCallable,
    ) : Activity1 {
      override fun activity() {
        LOGGER.info(BEFORE, ACTIVITY1)
        try {
          callable.call()
        } catch (e: Exception) {
          throw java.lang.RuntimeException(e)
        }
        LOGGER.info(BEFORE, ACTIVITY1)
      }

      companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(Activity1Impl::class.java)
        private const val ACTIVITY1 = "activity1"
      }
    }
  }

  @WorkflowInterface
  interface TestFailingWorkflow {
    @WorkflowMethod
    fun run(arg: String?): String?

    class WorkflowImpl : TestFailingWorkflow {
      val options: ActivityOptions =
        ActivityOptions
          .newBuilder()
          .setScheduleToCloseTimeout(Duration.ofMinutes(30))
          .setStartToCloseTimeout(Duration.ofMinutes(30))
          .setScheduleToStartTimeout(Duration.ofMinutes(30))
          .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
          .setRetryOptions(TemporalConstants.NO_RETRY)
          .setHeartbeatTimeout(Duration.ofSeconds(1))
          .build()

      private val activity1: Activity1 = Workflow.newActivityStub<Activity1>(Activity1::class.java, options)

      override fun run(arg: String?): String {
        LOGGER.info("workflow before activity 1")
        activity1.activity(arg)
        LOGGER.info("workflow after all activities")

        return "completed"
      }

      companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(WorkflowImpl::class.java)
      }
    }

    @ActivityInterface
    interface Activity1 {
      @ActivityMethod
      fun activity(arg: String?)
    }

    class Activity1Impl(
      private val timesReachedEnd: AtomicInteger,
    ) : Activity1 {
      override fun activity(arg: String?) {
        LOGGER.info(BEFORE, ACTIVITY1)
        val context = Activity.getExecutionContext()
        HeartbeatUtils.withBackgroundHeartbeat<Any?>(
          AtomicReference<Runnable>(null),
          Callable {
            if (timesReachedEnd.get() == 0) {
              if ("runtime" == arg) {
                throw java.lang.RuntimeException("failed")
              } else if ("timeout" == arg) {
                Thread.sleep(10000)
                return@Callable null
              } else {
                throw Exception("failed")
              }
            } else {
              return@Callable null
            }
          },
          context,
        )
        timesReachedEnd.incrementAndGet()
        LOGGER.info(BEFORE, ACTIVITY1)
      }

      companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(TestWorkflow.Activity1Impl::class.java)
        private const val ACTIVITY1 = "activity1"
      }
    }
  }

  companion object {
    private const val TASK_QUEUE = "default"
    private const val BEFORE = "before: {}"
  }
}
