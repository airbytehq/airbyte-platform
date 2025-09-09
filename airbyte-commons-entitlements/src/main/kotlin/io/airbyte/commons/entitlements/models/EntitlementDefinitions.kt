/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

object DestinationObjectStorageEntitlement : FeatureEntitlement(
  featureId = "feature-destination-object-storage",
)

object FasterSyncFrequencyEntitlement : FeatureEntitlement(
  featureId = "feature-faster-sync-frequency",
)

object SsoEntitlement : FeatureEntitlement(
  featureId = "feature-sso",
)

object OrchestrationEntitlement : FeatureEntitlement(
  featureId = "feature-orchestration",
)

object SelfManagedRegionsEntitlement : FeatureEntitlement(
  featureId = "feature-self-managed-regions",
)

object AiCopilotEntitlement : FeatureEntitlement(
  featureId = "feature-ai-copilot",
)

object MultipleWorkspacesEntitlement : FeatureEntitlement(
  featureId = "feature-multiple-workspaces",
)

object MappersEntitlement : FeatureEntitlement(
  featureId = "feature-mappers",
)

object RbacRolesEntitlement : FeatureEntitlement(
  featureId = "feature-rbac-roles",
)

object RejectedRecordsStorage : FeatureEntitlement(
  featureId = "feature-rejected-records-storage",
)

object Entitlements {
  private val ALL =
    listOf(
      FasterSyncFrequencyEntitlement,
      DestinationObjectStorageEntitlement,
      SsoEntitlement,
      OrchestrationEntitlement,
      SelfManagedRegionsEntitlement,
      AiCopilotEntitlement,
      MultipleWorkspacesEntitlement,
      MappersEntitlement,
      RbacRolesEntitlement,
      RejectedRecordsStorage,
    )

  private val byId = ALL.associateBy { it.featureId }

  fun fromId(featureId: String): Entitlement? =
    if (ConnectorEntitlement.isConnectorFeatureId(featureId)) {
      ConnectorEntitlement.fromFeatureId(featureId)
    } else {
      byId[featureId]
    }
}
