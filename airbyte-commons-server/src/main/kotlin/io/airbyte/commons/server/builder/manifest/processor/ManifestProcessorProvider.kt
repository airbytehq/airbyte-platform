/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.manifest.processor

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseManifestServer
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.WorkspaceHelper
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Determines the appropriate ManifestProcessor based on feature flags.
 *
 * Once the manifest-server is fully rolled out, this class and the ManifestProcessor interface can be removed.
 */
@Singleton
class ManifestProcessorProvider(
  private val featureFlagClient: FeatureFlagClient,
  private val builderServerProcessor: BuilderServerManifestProcessor,
  private val manifestServerProcessor: ManifestServerManifestProcessor,
  private val workspaceHelper: WorkspaceHelper,
) {
  fun getProcessor(workspaceId: UUID): ManifestProcessor {
    val orgId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
    val contexts = listOf(Organization(orgId), Workspace(workspaceId))
    return if (featureFlagClient.boolVariation(UseManifestServer, Multi(contexts))) {
      manifestServerProcessor
    } else {
      builderServerProcessor
    }
  }
}
