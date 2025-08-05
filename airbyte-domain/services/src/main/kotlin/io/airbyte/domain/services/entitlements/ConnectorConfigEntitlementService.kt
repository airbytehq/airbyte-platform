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

/**
 * Represents an entitlement requirement with optional discriminator to determine when a property is considered "set"
 */
data class EntitlementRequirement(
  val entitlement: Entitlement,
  val discriminator: ((JsonNode) -> Boolean)? = null,
) {
  /**
   * Determines if the given property value should be considered as "set" and thus requiring the entitlement
   */
  fun isPropertySet(propertyValue: JsonNode?): Boolean {
    if (propertyValue == null || propertyValue.isNull) {
      return false
    }
    return discriminator?.invoke(propertyValue) ?: true
  }
}

@Singleton
class ConnectorConfigEntitlementService(
  private val entitlementService: EntitlementService,
  private val connectorObjectStorageService: ConnectorObjectStorageService,
) {
  val log = KotlinLogging.logger {}

  private fun getEntitlementRequirements(actorDefinitionVersion: ActorDefinitionVersion): Map<String, EntitlementRequirement> {
    val objectStorageProperty = connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion)
    return buildMap {
      if (objectStorageProperty != null) {
        // Object storage discriminator: consider property not set if {"storage_type": "None"}
        val objectStorageDiscriminator: (JsonNode) -> Boolean = { propertyValue ->
          !(
            propertyValue.isObject &&
              propertyValue.has("storage_type") &&
              propertyValue.get("storage_type").asText() == "None"
          )
        }
        put(objectStorageProperty, EntitlementRequirement(DestinationObjectStorageEntitlement, objectStorageDiscriminator))
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
        .filterNot { (_, requirement) -> entitlementService.checkEntitlement(organizationId.value, requirement.entitlement).isEntitled }

    val spec = actorDefinitionVersion.spec

    // Create a modified spec annotated for the UI with entitlements considered
    val modifiedSpec = Jsons.clone(spec)
    val modifiedJsonSchema = modifiedSpec.connectionSpecification

    if (modifiedJsonSchema.has("properties")) {
      val properties = modifiedJsonSchema.get("properties") as ObjectNode

      for ((propertyName, requirement) in entitlementRequirements.entries) {
        if (properties.has(propertyName)) {
          val propertyNode = properties.get(propertyName) as ObjectNode

          // Always add airbyte_required_entitlement
          propertyNode.put("airbyte_required_entitlement", requirement.entitlement.featureId)

          // Check if the field is required
          val isRequired =
            modifiedJsonSchema.has("required") &&
              modifiedJsonSchema.get("required").isArray &&
              modifiedJsonSchema.get("required").any { it.asText() == propertyName }

          // Hide the property if the organization is not entitled
          if (missingEntitlements.any { it.value.entitlement.featureId == requirement.entitlement.featureId }) {
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

    return EntitledConnectorSpec(spec = modifiedSpec, missingEntitlements = missingEntitlements.map { it.value.entitlement.featureId })
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
        val propertyValue = if (config.has(property)) config.get(property) else null
        entitlementRequirements[property]?.isPropertySet(propertyValue) ?: false
      }

    for ((_, requirement) in usedProperties) {
      entitlementService.ensureEntitled(organizationId.value, requirement.entitlement)
    }
  }
}
