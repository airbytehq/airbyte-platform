/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.entitlements

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.DestinationObjectStorageEntitlement
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.domain.models.EntitledConnectorSpec
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.storage.ConnectorObjectStorageService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import kotlin.collections.iterator

@Singleton
class ConnectorConfigEntitlementService(
  private val entitlementService: EntitlementService,
  private val connectorObjectStorageService: ConnectorObjectStorageService,
) {
  val log = KotlinLogging.logger {}

  private fun getEntitlementRequirements(actorDefinitionVersion: ActorDefinitionVersion): Map<String, Entitlement> {
    val objectStorageProperty = connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion)
    return buildMap {
      if (objectStorageProperty != null) {
        put(objectStorageProperty, DestinationObjectStorageEntitlement)
      }
    }
  }

  /**
   * Returns mutated spec after having annotated entitlement requirements for the UI.
   */
  @JvmName("getEntitledConnectorSpec")
  fun getEntitledConnectorSpec(
    organizationId: OrganizationId,
    actorDefinitionVersion: ActorDefinitionVersion,
  ): EntitledConnectorSpec {
    val entitlementRequirements = getEntitlementRequirements(actorDefinitionVersion)

    val missingEntitlements =
      entitlementRequirements.entries
        .filterNot { (_, entitlement) -> entitlementService.checkEntitlement(organizationId.value, entitlement).isEntitled }

    val spec = actorDefinitionVersion.spec

    // Create a modified spec annotated for the UI with entitlements considered
    val modifiedSpec = Jsons.clone(spec)
    val modifiedJsonSchema = modifiedSpec.connectionSpecification

    if (modifiedJsonSchema.has("properties")) {
      val properties = modifiedJsonSchema.get("properties") as ObjectNode

      for ((propertyName, entitlement) in entitlementRequirements.entries) {
        if (properties.has(propertyName)) {
          val propertyNode = properties.get(propertyName) as ObjectNode

          // Always add airbyte_required_entitlement
          propertyNode.put("airbyte_required_entitlement", entitlement.featureId)

          // Check if the field is required
          val isRequired =
            modifiedJsonSchema.has("required") &&
              modifiedJsonSchema.get("required").isArray &&
              modifiedJsonSchema.get("required").any { it.asText() == propertyName }

          // Hide the property if the organization is not entitled
          if (missingEntitlements.any { it.value.featureId == entitlement.featureId }) {
            if (isRequired) {
              log.warn {
                "Property $propertyName is not entitled to be used, but is required in the spec. Due to this requirement, the field was not trimmed."
              }
            } else {
              propertyNode.put("airbyte_hidden", true)
            }
          }
        }
      }
    }

    return EntitledConnectorSpec(spec = modifiedSpec, missingEntitlements = missingEntitlements.map { it.value.featureId })
  }

  /**
   * Validates that features that require entitlements cannot be used in the config unless the org is entitled.
   */
  @JvmName("ensureEntitledConfig")
  fun ensureEntitledConfig(
    organizationId: OrganizationId,
    actorDefinitionVersion: ActorDefinitionVersion,
    config: JsonNode,
  ) {
    val entitlementRequirements = getEntitlementRequirements(actorDefinitionVersion)
    val usedProperties =
      entitlementRequirements.filterKeys { property ->
        config.has(property) && !config.get(property).isNull
      }

    for ((_, entitlement) in usedProperties) {
      entitlementService.ensureEntitled(organizationId.value, entitlement)
    }
  }
}
