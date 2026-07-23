/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.core.JsonParseException
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import io.airbyte.config.CustomerTier
import io.airbyte.data.config.GcsStorageProvider
import io.airbyte.data.exceptions.OrganizationAttributeException
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConnectorRolloutConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

private const val MILLIS_PER_HOUR = 60 * 60 * 1000L
private const val FILE_TIMESTAMP_MS = 1_000_000_000_000L
private const val FILE_AGE_HOURS = 3L
private const val NOW_MS = FILE_TIMESTAMP_MS + FILE_AGE_HOURS * MILLIS_PER_HOUR
private const val OBJECT_PREFIX = "data/organization_customer_tiers"
private const val BUCKET_NAME = "test-bucket"
private const val PROJECT_ID = "projectId"
private val PREFIX_OPTION = Storage.BlobListOption.prefix(OBJECT_PREFIX)

private fun fileName(timestampMs: Long) = "$OBJECT_PREFIX/YYYY_MM_DD_${timestampMs}_0.jsonl"

class OrganizationCustomerAttributesServiceDataImplTest {
  private lateinit var storageMock: Storage
  private lateinit var blobMock: Blob
  private lateinit var organizationCustomerAttributeService: OrganizationCustomerAttributesServiceDataImpl
  private lateinit var gcsStorageProvider: GcsStorageProvider
  private lateinit var metricClient: MetricClient

  @BeforeEach
  fun setUp() {
    storageMock = mockk()
    gcsStorageProvider = mockk<GcsStorageProvider>()
    blobMock = mockk()
    metricClient = mockk(relaxed = true)
    organizationCustomerAttributeService =
      OrganizationCustomerAttributesServiceDataImpl(
        AirbyteConnectorRolloutConfig(
          gcs =
            AirbyteConnectorRolloutConfig.AirbyteConnectorRolloutGcsConfig(
              applicationCredentials = "creds",
              bucketName = BUCKET_NAME,
              projectId = PROJECT_ID,
              objectPrefix = OBJECT_PREFIX,
            ),
        ),
        gcsStorageProvider = gcsStorageProvider,
        metricClient = metricClient,
        clock = Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC),
      )
  }

  @Test
  fun `test getOrganizationTiers throws when GCS credentials are missing`() {
    val serviceWithoutCreds =
      OrganizationCustomerAttributesServiceDataImpl(
        AirbyteConnectorRolloutConfig(
          gcs =
            AirbyteConnectorRolloutConfig.AirbyteConnectorRolloutGcsConfig(
              applicationCredentials = "",
              bucketName = BUCKET_NAME,
              projectId = PROJECT_ID,
            ),
        ),
        gcsStorageProvider = gcsStorageProvider,
        metricClient = metricClient,
        clock = Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC),
      )

    assertThrows<OrganizationAttributeException> { serviceWithoutCreds.getOrganizationTiers() }
    verify(exactly = 0) { gcsStorageProvider.provideStorage(any(), any()) }
  }

  @Test
  fun `test getOrganizationTiers throws when no files exist`() {
    every { gcsStorageProvider.provideStorage(any(), any()) } returns storageMock
    every { storageMock.list(BUCKET_NAME, *anyVararg()) } returns null

    assertThrows<OrganizationAttributeException> { organizationCustomerAttributeService.getOrganizationTiers() }

    verify { storageMock.list(BUCKET_NAME, PREFIX_OPTION) }
  }

  @Test
  fun `test getOrganizationTiers reads and parses the most recent file`() {
    val blob1 = mockk<Blob>()
    val blob2 = mockk<Blob>()

    every { blob1.name } returns fileName(FILE_TIMESTAMP_MS - 1) // older
    every { blob2.name } returns fileName(FILE_TIMESTAMP_MS) // most recent -> read
    every { gcsStorageProvider.provideStorage(any(), any()) } returns storageMock

    every { storageMock.list(BUCKET_NAME, PREFIX_OPTION) } returns
      mockk<Page<Blob>>().apply {
        every { iterateAll() } returns listOf(blob1, blob2)
      }

    every { blob2.getContent() } returns
      """
      {"_airbyte_data":{"organization_id":"00000000-0000-0000-0000-000000000000","customer_tier":"Tier 0"}}
      {"_airbyte_data":{"organization_id":"00000000-0000-0000-0000-000000000001","customer_tier":"Tier 1"}}
      {"_airbyte_data":{"organization_id":"00000000-0000-0000-0000-000000000002","customer_tier":"Tier 2"}}
      """.trimIndent().toByteArray()

    val result = organizationCustomerAttributeService.getOrganizationTiers()

    assertEquals(3, result.size)
    assertEquals(CustomerTier.TIER_1, result[UUID.fromString("00000000-0000-0000-0000-000000000001")])
    // most recent file timestamp is FILE_TIMESTAMP_MS and the clock is fixed FILE_AGE_HOURS later
    assertEquals(FILE_AGE_HOURS.toDouble(), organizationCustomerAttributeService.attributeAge.reportableValue())

    verify { storageMock.list(BUCKET_NAME, PREFIX_OPTION) }
  }

  @Test
  fun `test attribute age is NaN until the first successful read`() {
    assertTrue(organizationCustomerAttributeService.attributeAge.reportableValue().isNaN())
  }

  @Test
  fun `test computeAgeInHours returns hours between now and the file timestamp`() {
    // clock is fixed at FILE_TIMESTAMP_MS + FILE_AGE_HOURS
    assertEquals(FILE_AGE_HOURS.toDouble(), organizationCustomerAttributeService.ageInHours(FILE_TIMESTAMP_MS))
    assertEquals(0.5, organizationCustomerAttributeService.ageInHours(NOW_MS - MILLIS_PER_HOUR / 2))
  }

  @Test
  fun `test getMostRecentFile returns the most recent file`() {
    val blob1 = mockk<Blob>()
    val blob2 = mockk<Blob>()
    val blob3 = mockk<Blob>()

    every { blob1.name } returns fileName(FILE_TIMESTAMP_MS - 2)
    every { blob2.name } returns fileName(FILE_TIMESTAMP_MS + 3) // most recent
    every { blob3.name } returns fileName(FILE_TIMESTAMP_MS)

    every { storageMock.list(BUCKET_NAME, PREFIX_OPTION) } returns
      mockk<Page<Blob>>().apply {
        every { iterateAll() } returns listOf(blob1, blob2, blob3)
      }

    val mostRecentFile = organizationCustomerAttributeService.getMostRecentFile(storageMock)

    assertEquals(blob2, mostRecentFile.blob)
    assertEquals(FILE_TIMESTAMP_MS + 3, mostRecentFile.timestampMs)
    verify {
      storageMock.list(BUCKET_NAME, PREFIX_OPTION)
    }
  }

  @Test
  fun `test getMostRecentFile skips files with an unparseable timestamp`() {
    val valid = mockk<Blob>()
    val invalid = mockk<Blob>()

    every { valid.name } returns fileName(FILE_TIMESTAMP_MS)
    every { invalid.name } returns "$OBJECT_PREFIX._not_a_timestamp.jsonl"

    every { storageMock.list(BUCKET_NAME, PREFIX_OPTION) } returns
      mockk<Page<Blob>>().apply {
        every { iterateAll() } returns listOf(invalid, valid)
      }

    val mostRecentFile = organizationCustomerAttributeService.getMostRecentFile(storageMock)

    assertEquals(valid, mostRecentFile.blob)
    assertEquals(FILE_TIMESTAMP_MS, mostRecentFile.timestampMs)
  }

  @Test
  fun `test getMostRecentFile throws when no files exist`() {
    every { storageMock.list(BUCKET_NAME, PREFIX_OPTION) } returns null

    assertThrows<OrganizationAttributeException> { organizationCustomerAttributeService.getMostRecentFile(storageMock) }
  }

  @Test
  fun `test getMostRecentFile throws when no file has a parseable timestamp`() {
    val invalid = mockk<Blob>()
    every { invalid.name } returns "invalid_file_name.jsonl"

    every { storageMock.list(any<String>(), *anyVararg()) } returns
      mockk<Page<Blob>>().apply {
        every { iterateAll() } returns listOf(invalid)
      }

    assertThrows<OrganizationAttributeException> { organizationCustomerAttributeService.getMostRecentFile(storageMock) }
  }

  @Test
  fun `test extractTimestamp extracts valid timestamp`() {
    val result = organizationCustomerAttributeService.extractTimestamp(fileName(FILE_TIMESTAMP_MS))

    assertEquals(FILE_TIMESTAMP_MS, result)
  }

  @Test
  fun `test extractTimestamp returns null for invalid timestamp format`() {
    val fileName = "invalid_file_name.jsonl"

    val result = organizationCustomerAttributeService.extractTimestamp(fileName)

    assertNull(result)
  }

  @Test
  fun `test readFileContent parses valid file content`() {
    val blobMock = mockk<Blob>()
    val fileContent =
      """
      {"_airbyte_data":{"organization_id":"00000000-0000-0000-0000-000000000001","customer_tier":"Tier 1"}}
      {"_airbyte_data":{"organization_id":"00000000-0000-0000-0000-000000000002","customer_tier":"Tier 2"}}
      """.trimIndent()

    every { blobMock.getContent() } returns fileContent.toByteArray()

    val result = organizationCustomerAttributeService.readFileContent(blobMock)

    assertEquals(2, result.size)
    assertEquals(CustomerTier.TIER_1, result[UUID.fromString("00000000-0000-0000-0000-000000000001")])
    assertEquals(CustomerTier.TIER_2, result[UUID.fromString("00000000-0000-0000-0000-000000000002")])
  }

  @Test
  fun `test readFileContent throws exception on invalid file content`() {
    val blobMock = mockk<Blob>()
    val invalidContent = "invalid-json-content"

    every { blobMock.name } returns fileName(FILE_TIMESTAMP_MS)
    every { blobMock.getContent() } returns invalidContent.toByteArray()

    assertThrows<OrganizationAttributeException> { organizationCustomerAttributeService.readFileContent(blobMock) }
  }

  @Test
  fun `test parseJsonLine parses valid JSON line`() {
    val jsonLine = """{"_airbyte_data":{"organization_id":"00000000-0000-0000-0000-000000000001","customer_tier":"Tier 1"}}"""

    val result = organizationCustomerAttributeService.parseJsonLine(jsonLine)

    assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000001"), result?.organizationId)
    assertEquals(CustomerTier.TIER_1, result?.customerTier)
  }

  @Test
  fun `test parseJsonLine throws for invalid JSON`() {
    val jsonLine = """invalid-json"""

    assertThrows<JsonParseException> { organizationCustomerAttributeService.parseJsonLine(jsonLine) }
  }
}
