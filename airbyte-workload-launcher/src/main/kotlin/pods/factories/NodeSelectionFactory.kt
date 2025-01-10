package io.airbyte.workload.launcher.pods.factories

import io.airbyte.featureflag.AllowSpotInstances
import io.airbyte.featureflag.Attempt
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.airbyte.workload.launcher.pods.model.NodeSelection
import io.fabric8.kubernetes.api.model.Affinity
import io.fabric8.kubernetes.api.model.AffinityBuilder
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder
import io.fabric8.kubernetes.api.model.PreferredSchedulingTermBuilder
import io.fabric8.kubernetes.api.model.Toleration
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
data class NodeSelectionFactory(
  private val featureFlagClient: FeatureFlagClient,
  @Named("replicationPodTolerations") private val tolerations: List<Toleration>,
  @Named("spotToleration") private val spotToleration: Toleration,
  @Named("infraFlagContexts") private val infraFlagContexts: List<Context>,
) {
  fun createReplicationNodeSelection(
    nodeSelectors: Map<String, String>,
    allLabels: Map<String, String>,
  ): NodeSelection {
    if (!shouldAllowSpotInstances(allLabels)) {
      return NodeSelection(
        nodeSelectors = nodeSelectors,
        tolerations = tolerations.toList(),
        podAffinity = null,
      )
    } else {
      val affinity = buildSpotInstanceAffinity()
      val finalTolerations =
        tolerations.toMutableList().apply {
          add(spotToleration)
        }
      return NodeSelection(
        nodeSelectors = nodeSelectors,
        tolerations = finalTolerations,
        podAffinity = affinity,
      )
    }
  }

  fun createResetNodeSelection(
    nodeSelectors: Map<String, String>,
    allLabels: Map<String, String>,
  ): NodeSelection = NodeSelection(nodeSelectors = nodeSelectors, tolerations = tolerations.toList(), podAffinity = null)

  private fun shouldAllowSpotInstances(allLabels: Map<String, String>) =
    featureFlagClient.boolVariation(AllowSpotInstances, buildSpotInstanceFeatureFlagContext(allLabels))

  private fun buildSpotInstanceFeatureFlagContext(allLabels: Map<String, String>): Context {
    val context = infraFlagContexts.toMutableList()
    allLabels["connection_id"]?.let { connectionId -> context.add(Connection(connectionId)) }
    allLabels["workspace_id"]?.let { workspaceId -> context.add(Workspace(workspaceId)) }
    allLabels["attempt_id"]?.let { attemptNumber -> context.add(Attempt(attemptNumber)) }
    return Multi(context)
  }

  private fun buildSpotInstanceAffinity(): Affinity {
    return AffinityBuilder()
      .withNewNodeAffinity()
      .withPreferredDuringSchedulingIgnoredDuringExecution(
        PreferredSchedulingTermBuilder()
          .withNewPreference()
          .withMatchExpressions(
            NodeSelectorRequirementBuilder()
              .withKey(spotToleration.key)
              .withValues(spotToleration.value)
              .withOperator("In")
              .build(),
          )
          .endPreference()
          .withWeight(100)
          .build(),
      )
      .endNodeAffinity()
      .build()
  }
}
