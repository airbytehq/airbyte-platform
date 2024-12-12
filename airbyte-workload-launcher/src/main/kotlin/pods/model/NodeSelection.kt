package io.airbyte.workload.launcher.pods.model

import io.fabric8.kubernetes.api.model.Affinity
import io.fabric8.kubernetes.api.model.Toleration

data class NodeSelection(
  val nodeSelectors: Map<String, String>,
  val podAffinity: Affinity?,
  val tolerations: List<Toleration>,
)
