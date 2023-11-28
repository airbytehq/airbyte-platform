package io.airbyte.workload.launcher.pipeline.consumer

import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.queue.QueueActivity
import io.airbyte.commons.temporal.queue.QueueWorkflow
import io.airbyte.commons.temporal.queue.QueueWorkflowBase
import io.airbyte.config.messages.LauncherInputMessage

/**
 * Launcher Workflow implementation.
 *
 * Class needs to be
 */
open class LauncherWorkflowImpl : QueueWorkflowBase<LauncherInputMessage>(), QueueWorkflow<LauncherInputMessage> {
  @TemporalActivityStub(activityOptionsBeanName = "queueActivityOptions")
  override lateinit var activity: QueueActivity<LauncherInputMessage>
}
