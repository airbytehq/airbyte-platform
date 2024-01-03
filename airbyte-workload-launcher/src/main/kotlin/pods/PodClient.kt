package io.airbyte.workload.launcher.pods

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput

interface PodClient {
  fun podsExistForWorkload(workloadId: String): Boolean

  fun launchReplication(
    replicationInput: ReplicationInput,
    launcherInput: LauncherInput,
  )

  fun launchCheck(
    checkInput: CheckConnectionInput,
    launcherInput: LauncherInput,
  )

  fun deleteMutexPods(mutexKey: String): Boolean
}
