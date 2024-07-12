package io.airbyte.workers.temporal.sync

import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.DiscoverPostprocessInTemporal
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.Workspace
import io.airbyte.workers.temporal.activities.SyncFeatureFlagFetcherInput
import jakarta.inject.Singleton

@Singleton
class SyncFeatureFlagFetcherActivityImpl(private val featureFlagClient: FeatureFlagClient) : SyncFeatureFlagFetcherActivity {
  override fun shouldRunAsChildWorkflow(input: SyncFeatureFlagFetcherInput): Boolean {
    return featureFlagClient.boolVariation(DiscoverPostprocessInTemporal, getContext(input))
  }

  private fun getContext(input: SyncFeatureFlagFetcherInput): Context {
    return Multi(
      listOf(
        Workspace(input.workspaceId),
        Connection(input.connectionId),
        SourceDefinition(input.sourceDefinitionId),
      ),
    )
  }
}
