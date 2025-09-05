/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

object PlatformLlmSyncJobFailureExplanation : FeatureEntitlement(
  featureId = "feature-platform-llm-sync-job-failure-explanation",
)

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

object PlanPillEntitlement : FeatureEntitlement(
  featureId = "feature-plan-pill",
)

object DataWorkerCapacityEntitlement : FeatureEntitlement(
  featureId = "feature-data-worker-capacity",
)

object MultipleWorkspacesEntitlement : FeatureEntitlement(
  featureId = "feature-multiple-workspaces",
)

object MultipleUsersEntitlement : FeatureEntitlement(
  featureId = "feature-multiple-users",
)

object ExternalSecretsManagerEntitlement : FeatureEntitlement(
  featureId = "feature-external-secrets-manager",
)

object SecretReferencesEntitlement : FeatureEntitlement(
  featureId = "feature-secret-references",
)

object PrivateLinkEntitlement : FeatureEntitlement(
  featureId = "feature-privatelink",
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

object UserInvitationsEntitlement : FeatureEntitlement(
  featureId = "feature-user-invitations",
)

object DbtCloudIntegrationEntitlement : FeatureEntitlement(
  featureId = "feature-dbt-cloud-integration",
)

object Entitlements {
  private val ALL =
    listOf(
      PlatformLlmSyncJobFailureExplanation,
      FasterSyncFrequencyEntitlement,
      DestinationObjectStorageEntitlement,
      SsoEntitlement,
      OrchestrationEntitlement,
      SelfManagedRegionsEntitlement,
      AiCopilotEntitlement,
      PlanPillEntitlement,
      DataWorkerCapacityEntitlement,
      MultipleWorkspacesEntitlement,
      MultipleUsersEntitlement,
      ExternalSecretsManagerEntitlement,
      SecretReferencesEntitlement,
      PrivateLinkEntitlement,
      MappersEntitlement,
      RbacRolesEntitlement,
      RejectedRecordsStorage,
      UserInvitationsEntitlement,
      DbtCloudIntegrationEntitlement,
    )

  private val byId = ALL.associateBy { it.featureId }

  fun fromId(featureId: String): Entitlement? =
    if (ConnectorEntitlement.isConnectorFeatureId(featureId)) {
      ConnectorEntitlement.fromFeatureId(featureId)
    } else {
      byId[featureId]
    }
}
