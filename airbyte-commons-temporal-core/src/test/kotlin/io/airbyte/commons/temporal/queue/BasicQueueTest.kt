/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.queue

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.airbyte.metrics.MetricClient
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import io.temporal.workflow.Workflow
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch

// Payload for the Queue
@JsonDeserialize(builder = TestQueueInput.Builder::class)
data class TestQueueInput(
  val input: String,
) {
  // Using a builder here to prove that we can use a payload with non-nullable fields.
  data class Builder(
    var input: String? = null,
  ) {
    @Suppress("unused")
    fun input(input: String) = apply { this.input = input }

    @Suppress("unused")
    fun build() = TestQueueInput(input = input!!)
  }
}

// The actual consumer
class TestConsumer(
  val latch: CountDownLatch = CountDownLatch(1),
) : MessageConsumer<TestQueueInput> {
  override fun consume(input: TestQueueInput) {
    latch.countDown()
    println(input)
  }
}

// Test implementation, this is required to map the activities and for registering an implementation with temporal.
class TestWorkflowImpl : QueueWorkflowBase<TestQueueInput>() {
  override lateinit var activity: QueueActivity<TestQueueInput>

  init {
    initializeActivity<QueueActivity<TestQueueInput>>()
  }

  // reified is a trick to be able to retrieve the type reference on a generic class to pass it to temporal.
  private inline fun <reified W : QueueActivity<TestQueueInput>> initializeActivity() {
    // Initializing the activity the official temporal way to be able to run the temporal standard tests.
    // For an actual workflow, we'd want to inject the activities so this init wouldn't be needed.
    this.activity =
      Workflow.newActivityStub(
        W::class.java,
        ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build(),
      )
  }
}

class BasicQueueTest {
  companion object {
    const val QUEUE_NAME = "testQueue"

    lateinit var consumer: TestConsumer
    lateinit var activity: QueueActivityImpl<TestQueueInput>

    lateinit var testEnv: TestWorkflowEnvironment
    lateinit var worker: Worker
    lateinit var client: WorkflowClient

    @JvmStatic
    @BeforeAll
    fun setUp() {
      testEnv = TestWorkflowEnvironment.newInstance()
      worker = testEnv.newWorker(QUEUE_NAME)
      worker.registerWorkflowImplementationTypes(TestWorkflowImpl::class.java)
      client = testEnv.workflowClient

      consumer = TestConsumer()
      activity = spyk(QueueActivityImpl(consumer))
      worker.registerActivitiesImplementations(activity)
      testEnv.start()
    }

    @JvmStatic
    @AfterAll
    fun tearDown() {
      testEnv.close()
    }
  }

  @Test
  fun testRoundTrip() {
    val workflowClient = spyk(WorkflowClientWrapped(client, mockk<MetricClient>(relaxed = true)))
    val optionsSlot = slot<WorkflowOptions>()

    val producer = TemporalMessageProducer<TestQueueInput>(workflowClient)
    val message = TestQueueInput("boom!")
    val messageId = "myId"
    producer.publish(QUEUE_NAME, message, messageId)

    // Since publishing is async, wait on the latch
    consumer.latch.await()
    verify { activity.consume(Message(message)) }
    verify { workflowClient.newWorkflowStub<QueueWorkflow<TestQueueInput>>(any(), capture(optionsSlot)) }
    assertEquals(messageId, optionsSlot.captured.workflowId)
  }
}
