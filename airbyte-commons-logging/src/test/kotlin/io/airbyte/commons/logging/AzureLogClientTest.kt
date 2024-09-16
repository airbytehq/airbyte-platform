package io.airbyte.commons.logging

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.models.BlobItem
import io.airbyte.commons.storage.StorageBucketConfig
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

private const val LOG_PREFIX = "job-logging"
private const val LOG_PATH = "log-path"

private val storageBucketConfig =
  StorageBucketConfig(
    log = "log",
    state = "state",
    workloadOutput = "output",
    activityPayload = "payload",
  )

internal class AzureLogClientTest {
  @Test
  fun `tailing cloud logs with one blob`() {
    val blobName = "test-blob"
    val blob = mockk<BlobItem> { every { name } returns blobName }
    val blobClient =
      mockk<BlobClient> {
        every { openInputStream() } returns
          mockk {
            justRun { close() }
            every { readAllBytes() } returns "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15".toByteArray()
          }
      }
    val blobContainerClient =
      mockk<BlobContainerClient> {
        every { getBlobClient(blobName) } returns blobClient
        every { listBlobsByHierarchy("$LOG_PREFIX/$LOG_PATH/") } returns
          mockk { every { iterator() } returns mutableListOf(blob).iterator() }
      }
    val logClientFactory =
      mockk<AzureLogClientFactory> {
        every { get() } returns
          mockk {
            every { getBlobContainerClient(storageBucketConfig.log) } returns blobContainerClient
          }
      }
    val azureClient =
      AzureLogClient(
        azureClientFactory = logClientFactory,
        storageBucketConfig = storageBucketConfig,
        meterRegistry = null,
        jobLoggingCloudPrefix = LOG_PREFIX,
      )

    val lines = azureClient.tailCloudLogs(LOG_PATH, 10)
    assertEquals(10, lines.size)
    assertEquals(listOf("6", "7", "8", "9", "10", "11", "12", "13", "14", "15"), lines)
  }

  @Test
  fun `tailing cloud logs with multiple blobs`() {
    val blobName0 = "test-blob-0"
    val blobName1 = "test-blob-1"
    val blob0 = mockk<BlobItem> { every { name } returns blobName0 }
    val blob1 = mockk<BlobItem> { every { name } returns blobName1 }
    val blobClient0 =
      mockk<BlobClient> {
        every { openInputStream() } returns
          mockk {
            justRun { close() }
            every { readAllBytes() } returns "0\n1\n2\n3\n4\n5\n6\n7\n8\n9".toByteArray()
          }
      }
    val blobClient1 =
      mockk<BlobClient> {
        every { openInputStream() } returns
          mockk {
            justRun { close() }
            every { readAllBytes() } returns "95\n96\n97\n98\n99\n".toByteArray()
          }
      }
    val blobContainerClient =
      mockk<BlobContainerClient> {
        every { getBlobClient(blobName0) } returns blobClient0
        every { getBlobClient(blobName1) } returns blobClient1
        every { listBlobsByHierarchy("$LOG_PREFIX/$LOG_PATH/") } returns
          mockk { every { iterator() } returns mutableListOf(blob0, blob1).iterator() }
      }
    val logClientFactory =
      mockk<AzureLogClientFactory> {
        every { get() } returns
          mockk {
            every { getBlobContainerClient(storageBucketConfig.log) } returns blobContainerClient
          }
      }
    val azureClient =
      AzureLogClient(
        azureClientFactory = logClientFactory,
        storageBucketConfig = storageBucketConfig,
        meterRegistry = null,
        jobLoggingCloudPrefix = LOG_PREFIX,
      )

    val lines = azureClient.tailCloudLogs(LOG_PATH, 10)
    assertEquals(10, lines.size)
    assertEquals(listOf("5", "6", "7", "8", "9", "95", "96", "97", "98", "99"), lines)
  }

  @Test
  fun `log deletion`() {
    val blobName = "test-blob"
    val blob = mockk<BlobItem> { every { name } returns blobName }
    val blobClient = mockk<BlobClient> { justRun { delete() } }
    val blobContainerClient =
      mockk<BlobContainerClient> {
        every { getBlobClient(blobName) } returns blobClient
        every { listBlobsByHierarchy("$LOG_PREFIX/$LOG_PATH") } returns
          mockk { every { iterator() } returns mutableListOf(blob).iterator() }
      }
    val logClientFactory =
      mockk<AzureLogClientFactory> {
        every { get() } returns
          mockk {
            every { getBlobContainerClient(storageBucketConfig.log) } returns blobContainerClient
          }
      }
    val azureClient =
      AzureLogClient(
        azureClientFactory = logClientFactory,
        storageBucketConfig = storageBucketConfig,
        meterRegistry = null,
        jobLoggingCloudPrefix = LOG_PREFIX,
      )

    azureClient.deleteLogs(LOG_PATH)

    verify { blobClient.delete() }
  }
}
