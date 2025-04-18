/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import com.google.common.annotations.VisibleForTesting
import io.airbyte.config.CustomerTier
import io.airbyte.data.config.OrganizationCustomerAttributesServiceConfig
import io.airbyte.data.services.OrganizationCustomerAttributesService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.cache.annotation.Cacheable
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}
private const val NO_CUSTOMER_TIER = "No Customer Tier"
private const val NO_ORGANIZATION_ID = "No Organization Id"

@Singleton
open class OrganizationCustomerAttributesServiceDataImpl(
  @Value("\${airbyte.connector-rollout.gcs.bucket-name}") private val gcsBucketName: String?,
  @Value("\${airbyte.connector-rollout.gcs.application-credentials}") private val gcsApplicationCredentials: String?,
  @Value("\${airbyte.connector-rollout.gcs.project-id}") private val gcsProjectId: String?,
  @Named("customerTierStorage") private val organizationCustomerAttributeServiceConfig: OrganizationCustomerAttributesServiceConfig,
) : OrganizationCustomerAttributesService {
  @Cacheable("organization-customer-attributes")
  override fun getOrganizationTiers(): Map<UUID, CustomerTier?> {
    val storage = organizationCustomerAttributeServiceConfig.provideStorage(gcsApplicationCredentials, gcsProjectId)
    if (storage == null) {
      logger.warn { "OrganizationCustomerAttributesServiceDataImpl getOrganizationTiers: GCS credentials are missing or invalid." }
      return emptyMap()
    }

    val mostRecentFile = getMostRecentFile(storage)
    return if (mostRecentFile == null) {
      logger.warn { "OrganizationCustomerAttributesServiceDataImpl getOrganizationTiers: No files found in bucket $gcsBucketName." }
      emptyMap()
    } else {
      logger.info { "OrganizationCustomerAttributesServiceDataImpl getOrganizationTiers:  most recent file: ${mostRecentFile.name}" }
      readFileContent(mostRecentFile)
    }
  }

  @VisibleForTesting
  internal fun getMostRecentFile(storage: Storage): Blob? {
    val blobs = storage.list(gcsBucketName)?.iterateAll()
    return if (blobs == null) {
      null
    } else {
      blobs
        .filter { it.name.endsWith(".jsonl") }
        .maxByOrNull { extractTimestamp(it.name) }
    }
  }

  @VisibleForTesting
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

  @VisibleForTesting
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

  @VisibleForTesting
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
