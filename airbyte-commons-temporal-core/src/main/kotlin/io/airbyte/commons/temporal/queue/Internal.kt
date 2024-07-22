package io.airbyte.commons.temporal.queue

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * Message abstraction.
 *
 * We wrap the actual message to be able to pass metadata around.
 */
@JsonDeserialize(builder = Message.Builder::class)
class Message<T : Any> constructor(data: T) {
  // TODO this should be a data class, however, need to make the JsonTypeInfo annotation work

  // This enables passing T around
  @JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_ARRAY, use = JsonTypeInfo.Id.CLASS, property = "@bodyClass")
  val data: T

  init {
    this.data = data
  }

  /**
   * Builder for messages.
   *
   * A builder is needed in order to have data as non-nullable because the deserializer requires a no-arg constructor.
   */
  class Builder<T : Any>
    @JvmOverloads
    constructor(data: T? = null) {
      @JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_ARRAY, use = JsonTypeInfo.Id.CLASS, property = "@bodyClass")
      var data: T?

      init {
        this.data = data
      }

      fun data(data: T) = apply { this.data = data }

      fun build() = Message(data = data!!)
    }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as Message<*>

    if (data != other.data) return false

    return true
  }

  override fun hashCode(): Int {
    return data.hashCode()
  }
}

/**
 * Generic queue activity.
 */
@ActivityInterface
interface QueueActivity<T : Any> {
  @ActivityMethod
  fun consume(message: Message<T>)
}

/**
 * Generic temporal workflow interface for a message queue.
 */
@WorkflowInterface
interface QueueWorkflow<T : Any> {
  /**
   * Submits a message to the queue.
   */
  @WorkflowMethod
  fun publish(message: Message<T>)
}

/**
 * Generic temporal queue activity implementation.
 */
class QueueActivityImpl<T : Any>(private val messageConsumer: MessageConsumer<T>) : QueueActivity<T> {
  override fun consume(message: Message<T>) {
    messageConsumer.consume(message.data)
  }
}

/**
 * Generic temporal queue workflow base implementation.
 */
abstract class QueueWorkflowBase<T : Any> : QueueWorkflow<T> {
  /**
   * The consumer activity.
   *
   * It is abstract at this level to let the workflow implementation class decide how to implement the actual field. For example, this enables the use
   * of @TemporalActivityStub to enable dependency injection for the activties.
   */
  protected abstract val activity: QueueActivity<T>

  override fun publish(message: Message<T>) {
    activity.consume(message)
  }
}
