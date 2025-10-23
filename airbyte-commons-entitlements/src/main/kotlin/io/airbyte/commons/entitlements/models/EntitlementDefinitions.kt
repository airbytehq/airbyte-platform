/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

import java.util.UUID

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

object ConfigTemplateEntitlement : FeatureEntitlement(
  featureId = "feature-embedded",
)

object DestinationSalesforceEnterpriseConnector : ConnectorEntitlement(
  actorDefinitionId = UUID.fromString("c0b24000-d34d-b33f-fea7-6b96dc0e5f0d"),
) {
  override val name: String = "destination-salesforce"
}

object SourceNetsuiteEnterpriseConnector : ConnectorEntitlement(
  actorDefinitionId = UUID.fromString("b979cb59-34b3-4f8b-9bf7-ae82d6371e2a"),
) {
  override val name: String = "source-netsuite"
}

object SourceOracleEnterpriseConnector : ConnectorEntitlement(
  actorDefinitionId = UUID.fromString("196a42fc-39f2-473f-88ff-d68b2ea702e9"),
) {
  override val name: String = "source-oracle"
}

object SourceSapHanaEnterpriseConnector : ConnectorEntitlement(
  actorDefinitionId = UUID.fromString("b6935898-aadb-46ae-99ca-d697207994c1"),
) {
  override val name: String = "source-sap-hana"
}

object SourceServicenowEnterpriseConnector : ConnectorEntitlement(
  actorDefinitionId = UUID.fromString("23867633-144a-4d7f-845a-a8bd9b9b9e5d"),
) {
  override val name: String = "source-servicenow"
}

object SourceSharepointEnterpriseConnector : ConnectorEntitlement(
  actorDefinitionId = UUID.fromString("c8e0fa7d-47a2-4f1f-b69b-03860f528263"),
) {
  override val name: String = "source-sharepoint"
}

object SourceWorkdayEnterpriseConnector : ConnectorEntitlement(
  actorDefinitionId = UUID.fromString("7b8b9550-331c-46c8-a299-943fb6ae2a72"),
) {
  override val name: String = "source-workday"
}

object SourceDb2EnterpriseConnector : ConnectorEntitlement(
  actorDefinitionId = UUID.fromString("d2542966-8cc8-4899-9b74-413a7d9bb28e"),
) {
  override val name: String = "source-db2"
}

object Entitlements {
  private val ALL: List<Entitlement> =
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
      ConfigTemplateEntitlement,
      DestinationSalesforceEnterpriseConnector,
      SourceNetsuiteEnterpriseConnector,
      SourceOracleEnterpriseConnector,
      SourceSapHanaEnterpriseConnector,
      SourceServicenowEnterpriseConnector,
      SourceSharepointEnterpriseConnector,
      SourceWorkdayEnterpriseConnector,
      SourceDb2EnterpriseConnector,
    )

  private val BY_FEATURE_ID: Map<String, Entitlement> =
    ALL.associateBy { it.featureId }

  private val CONNECTORS_BY_ACTOR_ID: Map<UUID, ConnectorEntitlement> =
    ALL
      .filterIsInstance<ConnectorEntitlement>()
      .associateBy { it.actorDefinitionId }

  fun fromId(featureId: String): Entitlement? {
    val parsedUuid = ConnectorEntitlement.parseActorDefinitionIdOrNull(featureId)
    return if (parsedUuid != null) {
      CONNECTORS_BY_ACTOR_ID[parsedUuid]
    } else {
      BY_FEATURE_ID[featureId]
    }
  }

  fun isEnterpriseConnectorEntitlementId(featureId: String): Boolean {
    val parsedUuid = ConnectorEntitlement.parseActorDefinitionIdOrNull(featureId)
    return CONNECTORS_BY_ACTOR_ID.containsKey(parsedUuid)
  }

  fun isEnterpriseSourceConnectorEntitlementId(featureId: String): Boolean {
    val parsedUuid = ConnectorEntitlement.parseActorDefinitionIdOrNull(featureId)
    return CONNECTORS_BY_ACTOR_ID[parsedUuid]?.name?.startsWith(ConnectorEntitlement.SOURCE_PREFIX) ?: false
  }

  fun connectorFromActorDefinitionId(id: UUID): ConnectorEntitlement? = CONNECTORS_BY_ACTOR_ID[id]

  fun actorDefinitionIdFromFeatureId(featureId: String): UUID? {
    val parsedUuid = ConnectorEntitlement.parseActorDefinitionIdOrNull(featureId)
    return CONNECTORS_BY_ACTOR_ID[parsedUuid]?.actorDefinitionId
  }
}
