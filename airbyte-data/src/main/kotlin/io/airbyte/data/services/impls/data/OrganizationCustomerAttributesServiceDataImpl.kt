/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.config.CustomerTier
import io.airbyte.data.config.OrganizationCustomerAttributesServiceConfig
import io.airbyte.data.services.OrganizationCustomerAttributesService
import io.airbyte.micronaut.runtime.AirbyteConnectorRolloutConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.cache.annotation.Cacheable
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}
private const val NO_CUSTOMER_TIER = "No Customer Tier"
private const val NO_ORGANIZATION_ID = "No Organization Id"

@Singleton
open class OrganizationCustomerAttributesServiceDataImpl(
  private val airbyteConnectorRolloutConfig: AirbyteConnectorRolloutConfig,
  @Named("customerTierStorage") private val organizationCustomerAttributeServiceConfig: OrganizationCustomerAttributesServiceConfig,
) : OrganizationCustomerAttributesService {
  @Cacheable("organization-customer-attributes")
  override fun getOrganizationTiers(): Map<UUID, CustomerTier?> {
    val storage =
      organizationCustomerAttributeServiceConfig.provideStorage(
        airbyteConnectorRolloutConfig.gcs.applicationCredentials,
        airbyteConnectorRolloutConfig.gcs.projectId,
      )
    if (storage == null) {
      logger.warn { "OrganizationCustomerAttributesServiceDataImpl getOrganizationTiers: GCS credentials are missing or invalid." }
      return emptyMap()
    }

    val mostRecentFile = getMostRecentFile(storage)
    return if (mostRecentFile == null) {
      logger.warn {
        "OrganizationCustomerAttributesServiceDataImpl getOrganizationTiers: No files found in bucket ${airbyteConnectorRolloutConfig.gcs.bucketName}."
      }
      emptyMap()
    } else {
      logger.info { "OrganizationCustomerAttributesServiceDataImpl getOrganizationTiers:  most recent file: ${mostRecentFile.name}" }
      readFileContent(mostRecentFile)
    }
  }

  @InternalForTesting
  internal fun getMostRecentFile(storage: Storage): Blob? {
    val blobs = storage.list(airbyteConnectorRolloutConfig.gcs.bucketName)?.iterateAll()
    return if (blobs == null) {
      null
    } else {
      blobs
        .filter { it.name.endsWith(".jsonl") }
        .maxByOrNull { extractTimestamp(it.name) }
    }
  }

  @InternalForTesting
  internal fun extractTimestamp(fileName: String): Long {
    logger.info { "OrganizationCustomerAttributesServiceDataImpl.extractTimestamp fileName=$fileName" }
    return try {
      val timestampPart = fileName.split("_").getOrNull(5)
      timestampPart?.toLongOrNull() ?: 0L
    } catch (e: Exception) {
      logger.warn { "OrganizationCustomerAttributesServiceDataImpl Failed to extract timestamp from file name: $fileName" }
      0L
    }
  }

  @InternalForTesting
  internal fun readFileContent(blob: Blob): Map<UUID, CustomerTier?> =
    try {
      val content = blob.getContent()
      val jsonLines = String(content).lines().filter { it.isNotBlank() }
      jsonLines
        .mapNotNull { parseJsonLine(it) }
        .associate { it.organizationId to it.customerTier }
    } catch (e: Exception) {
      logger.error(e) { "OrganizationCustomerAttributesServiceDataImpl Failed to read content of the file: ${blob.name}" }
      emptyMap()
    }

  @InternalForTesting
  internal fun parseJsonLine(line: String): OrganizationCustomerTierMapping? {
    val jsonObject = jacksonObjectMapper().readTree(line)
    val organizationIdString = jsonObject["_airbyte_data"]?.get("organization_id")?.asText() ?: return null
    val customerTierString = jsonObject["_airbyte_data"]?.get("customer_tier")?.asText() ?: return null

    return if (organizationIdString == NO_ORGANIZATION_ID || customerTierString == NO_CUSTOMER_TIER) {
      null
    } else {
      OrganizationCustomerTierMapping(
        organizationId = UUID.fromString(organizationIdString),
        customerTier = CustomerTier.valueOf(customerTierString.replace(" ", "_").uppercase()),
      )
    }
  }
}

data class OrganizationCustomerTierMapping(
  val organizationId: UUID,
  val customerTier: CustomerTier?,
)
