package io.airbyte.workload.launcher.pods

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import java.util.UUID

interface PodClient {
  fun podsExistForAutoId(autoId: UUID): Boolean

  fun launchReplication(
    replicationInput: ReplicationInput,
    launcherInput: LauncherInput,
  )

  fun launchCheck(
    checkInput: CheckConnectionInput,
    launcherInput: LauncherInput,
  )

  fun launchDiscover(
    discoverCatalogInput: DiscoverCatalogInput,
    launcherInput: LauncherInput,
  )

  fun launchSpec(
    specInput: SpecInput,
    launcherInput: LauncherInput,
  )

  fun deleteMutexPods(mutexKey: String): Boolean
}
