/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.config.CustomerTier
import io.airbyte.data.config.GcsStorageProvider
import io.airbyte.data.exceptions.OrganizationAttributeException
import io.airbyte.data.services.OrganizationCustomerAttributesService
import io.airbyte.metrics.ExpiringGaugeValue
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.micronaut.runtime.AirbyteConnectorRolloutConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.cache.annotation.Cacheable
import jakarta.inject.Singleton
import java.time.Clock
import java.util.UUID

private val logger = KotlinLogging.logger {}
private const val MILLISECONDS_PER_HOUR = 60 * 60 * 1000.0 // 3,600,000
private const val EXPIRING_METRIC_MILLISECONDS = 5 * 60 * 1000L // 5 minutes

@Singleton
open class OrganizationCustomerAttributesServiceDataImpl(
  private val airbyteConnectorRolloutConfig: AirbyteConnectorRolloutConfig,
  private val gcsStorageProvider: GcsStorageProvider,
  metricClient: MetricClient,
  private val clock: Clock = Clock.systemUTC(),
) : OrganizationCustomerAttributesService {
  // Self-expiring gauge values for the organization attributes file. Updated on every getOrganizationTiers() call
  // (cache hit or miss). Report NaN (skipped by Micrometer) if no call has happened within EXPIRING_METRIC_MILLISECONDS,
  // so the metrics track age and count while in use and stop publishing when stale.
  @InternalForTesting
  internal val attributeAge = ExpiringGaugeValue(clock, EXPIRING_METRIC_MILLISECONDS)

  @InternalForTesting
  internal val orgCount = ExpiringGaugeValue(clock, EXPIRING_METRIC_MILLISECONDS)

  init {
    metricClient.gauge(
      OssMetricsRegistry.ORGANIZATION_ATTRIBUTE_AGE_HOURS,
      attributeAge,
      { it.reportableValue() },
    )
    metricClient.gauge(
      OssMetricsRegistry.ORGANIZATION_ATTRIBUTE_COUNT,
      orgCount,
      { it.reportableValue() },
    )
  }

  override fun getOrganizationTiers(): Map<UUID, CustomerTier?> {
    val attributes = loadOrganizationAttributes()
    attributeAge.record(ageInHours(attributes.fileTimestampMs))
    orgCount.record(attributes.tiers.count().toDouble())
    return attributes.tiers
  }

  @Cacheable("organization-customer-attributes")
  open fun loadOrganizationAttributes(): OrganizationAttributes {
    val credentials = airbyteConnectorRolloutConfig.gcs.applicationCredentials
    val projectId = airbyteConnectorRolloutConfig.gcs.projectId
    if (credentials.isBlank()) {
      throw OrganizationAttributeException(
        "Cannot resolve organization customer tiers: GCS application credentials are missing.",
      )
    }
    if (projectId.isBlank()) {
      throw OrganizationAttributeException(
        "Cannot resolve organization customer tiers: GCS project id is missing.",
      )
    }

    val storage = gcsStorageProvider.provideStorage(credentials, projectId)

    val mostRecentFile = getMostRecentFile(storage)

    logger.info { "OrganizationCustomerAttributesServiceDataImpl loadOrganizationAttributes: most recent file: ${mostRecentFile.blob.name}" }
    return OrganizationAttributes(
      tiers = readFileContent(mostRecentFile.blob),
      fileTimestampMs = mostRecentFile.timestampMs,
    )
  }

  @InternalForTesting
  internal fun ageInHours(fileTimestampMs: Long): Double = (clock.millis() - fileTimestampMs) / MILLISECONDS_PER_HOUR

  @InternalForTesting
  internal fun getMostRecentFile(storage: Storage): TimestampedBlob =
    storage
      .list(
        airbyteConnectorRolloutConfig.gcs.bucketName,
        Storage.BlobListOption.prefix(airbyteConnectorRolloutConfig.gcs.objectPrefix),
      )?.iterateAll()
      ?.filter { it.name.endsWith(".jsonl") }
      ?.mapNotNull { blob -> extractTimestamp(blob.name)?.let { TimestampedBlob(blob, it) } }
      ?.maxByOrNull { it.timestampMs }
      ?: throw OrganizationAttributeException(
        "Cannot resolve organization customer tiers: no .jsonl files with expected naming pattern found in bucket " +
          "${airbyteConnectorRolloutConfig.gcs.bucketName} with prefix ${airbyteConnectorRolloutConfig.gcs.objectPrefix}.",
      )

  @InternalForTesting
  internal fun extractTimestamp(fileName: String): Long? = fileName.split("_").getOrNull(5)?.toLongOrNull()

  @InternalForTesting
  internal fun readFileContent(blob: Blob): Map<UUID, CustomerTier?> =
    try {
      val content = blob.getContent()
      val jsonLines = String(content).lines().filter { it.isNotBlank() }
      jsonLines
        .mapNotNull { parseJsonLine(it) }
        .associate { it.organizationId to it.customerTier }
    } catch (e: Exception) {
      throw OrganizationAttributeException("Failed to read or parse customer tier file: ${blob.name}", e)
    }

  @InternalForTesting
  internal fun parseJsonLine(line: String): OrganizationCustomerTierMapping? {
    val jsonObject = jacksonObjectMapper().readTree(line)
    val organizationIdString = jsonObject["_airbyte_data"]?.get("organization_id")?.asText() ?: return null
    val customerTierString = jsonObject["_airbyte_data"]?.get("customer_tier")?.asText() ?: return null

    return OrganizationCustomerTierMapping(
      organizationId = UUID.fromString(organizationIdString),
      customerTier = CustomerTier.valueOf(customerTierString.replace(" ", "_").uppercase()),
    )
  }
}

data class OrganizationCustomerTierMapping(
  val organizationId: UUID,
  val customerTier: CustomerTier?,
)

data class TimestampedBlob(
  val blob: Blob,
  val timestampMs: Long,
)

/**
 * Cached payload for [OrganizationCustomerAttributesService.getOrganizationTiers]. Carries the source file's
 * timestamp alongside the tier map so the freshness metric can be computed even when the value is served from cache.
 */
data class OrganizationAttributes(
  val tiers: Map<UUID, CustomerTier?>,
  val fileTimestampMs: Long,
)
