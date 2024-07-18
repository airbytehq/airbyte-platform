package io.airbyte.commons.temporal.queue

import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.temporal.client.WorkflowOptions

/**
 * Generic message producer for a temporal based queue.
 */
class TemporalMessageProducer<T : Any>(private val workflowClientWrapped: WorkflowClientWrapped) {
  /**
   * Publish a message to the subject.
   */
  fun publish(
    subject: String,
    message: T,
    messageId: String,
  ) {
    doPublish<QueueWorkflow<T>>(subject, message, messageId)
  }

  // This is a workaround to get a class with a generic.
  // Temporal newWorkflowStub call requires a Class<T>, reified enables this.
  // This is added as a private fun because of the visibility constraint from inline on member access.
  private inline fun <reified W : QueueWorkflow<T>> doPublish(
    subject: String,
    message: T,
    messageId: String,
  ) {
    val workflowOptions =
      WorkflowOptions.newBuilder()
        .setTaskQueue(subject)
        .setWorkflowId(messageId)
        .build()
    val workflow = workflowClientWrapped.newWorkflowStub(W::class.java, workflowOptions)
    workflowClientWrapped.start(workflow::publish, Message(message))
  }
}
