/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.core.JsonParseException
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import io.airbyte.config.CustomerTier
import io.airbyte.data.config.OrganizationCustomerAttributesServiceConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

private val logger = KotlinLogging.logger {}

class OrganizationCustomerAttributesServiceDataImplTest {
  private lateinit var storageMock: Storage
  private lateinit var blobMock: Blob
  private lateinit var organizationCustomerAttributeService: OrganizationCustomerAttributesServiceDataImpl
  private lateinit var organizationCustomerAttributeServiceConfig: OrganizationCustomerAttributesServiceConfig

  @BeforeEach
  fun setUp() {
    storageMock = mockk()
    organizationCustomerAttributeServiceConfig = mockk<OrganizationCustomerAttributesServiceConfig>()
    blobMock = mockk()
    organizationCustomerAttributeService =
      OrganizationCustomerAttributesServiceDataImpl(
        gcsBucketName = "test-bucket",
        gcsApplicationCredentials = "creds",
        gcsProjectId = "projectId",
        organizationCustomerAttributeServiceConfig = organizationCustomerAttributeServiceConfig,
      )
  }

  @Test
  fun `test getOrganizationTiers returns empty map when storage is null`() {
    every { organizationCustomerAttributeServiceConfig.provideStorage(any(), any()) } returns null

    val result = organizationCustomerAttributeService.getOrganizationTiers()

    assertTrue(result.isEmpty())
    logger.info { "Storage is null: Result verified as empty map." }
  }

  @Test
  fun `test getOrganizationTiers returns empty map when no files exist`() {
    every { organizationCustomerAttributeServiceConfig.provideStorage(any(), any()) } returns storageMock
    every { storageMock.list("test-bucket") } returns null

    val result = organizationCustomerAttributeService.getOrganizationTiers()

    assertTrue(result.isEmpty())
    verify { storageMock.list("test-bucket") }
  }

  @Test
  fun `test getOrganizationTiers reads and parses the most recent file`() {
    val blob1 = mockk<Blob>()
    val blob2 = mockk<Blob>()

    every { blob1.name } returns "data/sales_customer_attributes/2024_11_24_1732490206043_0.jsonl"
    every { blob2.name } returns "data/sales_customer_attributes/2024_11_24_1732490206044_0.jsonl"
    every { organizationCustomerAttributeServiceConfig.provideStorage(any(), any()) } returns storageMock

    every { storageMock.list("test-bucket") } returns
      mockk<Page<Blob>>().apply {
        every { iterateAll() } returns listOf(blob1, blob2)
      }

    every { blob2.getContent() } returns
      """
      {"_airbyte_data":{"organization_id":"00000000-0000-0000-0000-000000000001","customer_tier":"Tier 1"}}
      {"_airbyte_data":{"organization_id":"00000000-0000-0000-0000-000000000002","customer_tier":"No Customer Tier"}}
      {"_airbyte_data":{"organization_id":"No Organization Id","customer_tier":"No Customer Tier"}}
      {"_airbyte_data":{"organization_id":"No Organization Id","customer_tier":"Tier 1"}}
      """.trimIndent().toByteArray()

    val result = organizationCustomerAttributeService.getOrganizationTiers()

    assertEquals(1, result.size) // Only valid entries should be included
    assertEquals(CustomerTier.TIER_1, result[UUID.fromString("00000000-0000-0000-0000-000000000001")])

    verify { storageMock.list("test-bucket") }
  }

  @Test
  fun `test getMostRecentFile returns the most recent file`() {
    val blob1 = mockk<Blob>()
    val blob2 = mockk<Blob>()
    val blob3 = mockk<Blob>()

    every { blob1.name } returns "data/sales_customer_attributes/2024_11_24_1732490206041_0.jsonl"
    every { blob2.name } returns "data/sales_customer_attributes/2024_11_24_1732490206047_0.jsonl"
    every { blob3.name } returns "data/sales_customer_attributes/2024_11_24_1732490206044_0.jsonl"

    every { storageMock.list("test-bucket") } returns
      mockk<Page<Blob>>().apply {
        every { iterateAll() } returns listOf(blob1, blob2, blob3)
      }

    val mostRecentFile = organizationCustomerAttributeService.getMostRecentFile(storageMock)

    assertEquals(blob2, mostRecentFile)
    verify {
      storageMock.list("test-bucket")
    }
  }

  @Test
  fun `test getMostRecentFile returns the most recent file when no files exist`() {
    every { organizationCustomerAttributeServiceConfig.provideStorage(any(), any()) } returns storageMock
    every { storageMock.list("test-bucket") } returns null

    val mostRecentFile = organizationCustomerAttributeService.getMostRecentFile(storageMock)

    assertEquals(null, mostRecentFile)
  }

  @Test
  fun `test extractTimestamp extracts valid timestamp`() {
    val fileName = "data/sales_customer_attributes/2024_11_24_1732490206044_0.jsonl"

    val result = organizationCustomerAttributeService.extractTimestamp(fileName)

    assertEquals(1732490206044, result)
  }

  @Test
  fun `test extractTimestamp handles invalid timestamp format`() {
    val fileName = "invalid_file_name.jsonl"

    val result = organizationCustomerAttributeService.extractTimestamp(fileName)

    assertEquals(0L, result)
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

    every { blobMock.getContent() } returns invalidContent.toByteArray()

    val result = organizationCustomerAttributeService.readFileContent(blobMock)

    assertEquals(emptyMap<UUID, CustomerTier?>(), result)
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
