/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.models.BlobItem
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import io.airbyte.commons.storage.STORAGE_TYPE
import io.airbyte.commons.storage.StorageBucketConfig
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.apache.commons.io.input.ReversedLinesFileReader
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.util.function.Consumer
import kotlin.io.path.deleteExisting

private val logger = KotlinLogging.logger {}

interface LogClient {
  fun deleteLogs(logPath: String)

  fun tailCloudLogs(
    logPath: String,
    numLines: Int,
  ): List<String>
}

private fun sanitisePath(
  prefix: String,
  path: String,
): String = Paths.get(prefix, path).toString()

interface CloudLogClient : LogClient

enum class LogClientType {
  AZURE,
  GCS,
  LOCAL,
  MINIO,
  S3,
}

private fun MeterRegistry?.createCounter(
  metricName: String,
  logClientType: LogClientType,
): Counter? =
  this?.let {
    Counter
      .builder(metricName)
      .tags(MetricTags.LOG_CLIENT_TYPE, logClientType.name.lowercase())
      .register(it)
  }

private fun <T : List<Any>> MeterRegistry?.createGauge(
  metricName: String,
  logClientType: LogClientType,
  stateObject: T,
): T =
  this?.gauge(metricName, listOf(Tag.of(MetricTags.LOG_CLIENT_TYPE, logClientType.name.lowercase())), stateObject, { stateObject.size.toDouble() })
    ?: stateObject

private fun MeterRegistry?.createTimer(
  metricName: String,
  logClientType: LogClientType,
): Timer? =
  this?.let {
    Timer
      .builder(metricName)
      .tags(MetricTags.LOG_CLIENT_TYPE, logClientType.name.lowercase())
      .register(it)
  }

@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^azure$")
@Named("azureLogClient")
class AzureLogClient(
  private val azureClientFactory: AzureLogClientFactory,
  private val storageBucketConfig: StorageBucketConfig,
  private val meterRegistry: MeterRegistry?,
  @Value("\${airbyte.logging.client.cloud.log-prefix:job-logging}") private val jobLoggingCloudPrefix: String,
) : CloudLogClient {
  override fun tailCloudLogs(
    logPath: String,
    numLines: Int,
  ): List<String> {
    // azure needs the trailing `/` in order for the list call to return the correct blobs
    val cloudLogPath = sanitisePath(jobLoggingCloudPrefix, logPath) + "/"
    logger.debug { "Tailing logs from azure path: $cloudLogPath" }

    logger.debug { "Tailing $numLines lines from logs from GCS path: $cloudLogPath" }
    val blobContainerClient = azureClientFactory.get().getBlobContainerClient(storageBucketConfig.log)

    val descending: MutableList<BlobItem> = blobContainerClient.listBlobsByHierarchy(cloudLogPath).reversed().toMutableList()

    logger.debug { "Start Azure list request" }

    val instrumentedDescendingTimestampBlobs =
      meterRegistry.createGauge(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVED.metricName,
        logClientType = LogClientType.GCS,
        stateObject = descending,
      )
    val timer =
      meterRegistry.createTimer(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVAL_TIME_MS.metricName,
        logClientType = LogClientType.GCS,
      )

    val result =
      if (timer != null) {
        timer.recordCallable { retrieveFiles(client = blobContainerClient, blobs = instrumentedDescendingTimestampBlobs, numLines = numLines) }
      } else {
        retrieveFiles(client = blobContainerClient, blobs = instrumentedDescendingTimestampBlobs, numLines = numLines)
      }
    logger.debug { "Done retrieving GCS logs: $logPath." }

    return result
  }

  override fun deleteLogs(logPath: String) {
    val cloudLogPath = sanitisePath(jobLoggingCloudPrefix, logPath)

    logger.info { "Start azure list and delete request for path: $cloudLogPath." }
    val blobContainerClient = azureClientFactory.get().getBlobContainerClient(storageBucketConfig.log)
    blobContainerClient.listBlobsByHierarchy(cloudLogPath).forEach {
      blobContainerClient.getBlobClient(it.name).delete()
    }
    logger.info { "Finished all deletes." }
  }

  private fun retrieveFiles(
    client: BlobContainerClient,
    blobs: List<BlobItem>,
    numLines: Int,
  ): List<String> {
    val lineCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_COUNT_RETRIEVED.metricName,
        logClientType = LogClientType.AZURE,
      )
    val byteCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_BYTES_RETRIEVED.metricName,
        logClientType = LogClientType.AZURE,
      )

    val lines = mutableListOf<String>()
    // run is necessary to allow the forEach calls to return early
    run {
      // iterate through blobs in descending order (oldest first)
      blobs.forEach { blob ->
        val blobClient = client.getBlobClient(blob.name)
        val currentFileLines =
          blobClient
            .openInputStream()
            .use { it.readAllBytes() }
            .toString(Charsets.UTF_8)
            .split("\n".toRegex())
            .dropLastWhile { it.isEmpty() }
            .toTypedArray()

        currentFileLines.reversed().forEach { line ->
          lines.add(line)
          lineCounter?.increment()
          byteCounter?.increment(line.length.toDouble())

          if (lines.size >= numLines) {
            return@run
          }
        }
      }
    }

    // finally reverse the lines so they're returned in ascending order
    lines.reverse()
    return lines
  }
}

@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^gcs$")
@Named("gcsLogClient")
class GscLogClient(
  private val gcsClientFactory: GcsLogClientFactory,
  private val storageBucketConfig: StorageBucketConfig,
  private val meterRegistry: MeterRegistry?,
  @Value("\${airbyte.logging.client.cloud.log-prefix:job-logging}") private val jobLoggingCloudPrefix: String,
) : CloudLogClient {
  override fun tailCloudLogs(
    logPath: String,
    numLines: Int,
  ): List<String> {
    val cloudLogPath = sanitisePath(jobLoggingCloudPrefix, logPath)

    logger.debug { "Tailing $numLines lines from logs from GCS path: $cloudLogPath" }
    val gcsClient: Storage = gcsClientFactory.get()

    logger.debug { "Start GCS list request." }

    val descending = mutableListOf<Blob>()
    gcsClient
      .list(
        storageBucketConfig.log,
        Storage.BlobListOption.prefix(cloudLogPath),
      ).iterateAll()
      .forEach(Consumer { e: Blob -> descending.addFirst(e) })

    logger.debug { "Start getting GCS objects." }
    return tailCloudLogSerially(descending, cloudLogPath, numLines)
  }

  private fun tailCloudLogSerially(
    descendingTimestampBlobs: List<Blob>,
    logPath: String,
    numLines: Int,
  ): List<String> {
    val instrumentedDescendingTimestampBlobs =
      meterRegistry.createGauge(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVED.metricName,
        logClientType = LogClientType.GCS,
        stateObject = descendingTimestampBlobs,
      )
    val timer =
      meterRegistry.createTimer(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVAL_TIME_MS.metricName,
        logClientType = LogClientType.GCS,
      )

    val result =
      if (timer != null) {
        timer.recordCallable { retrieveFiles(blobs = instrumentedDescendingTimestampBlobs, numLines = numLines) }
      } else {
        retrieveFiles(blobs = instrumentedDescendingTimestampBlobs, numLines = numLines)
      }
    logger.debug { "Done retrieving GCS logs: $logPath." }
    return result
  }

  override fun deleteLogs(logPath: String) {
    val cloudLogPath = sanitisePath(jobLoggingCloudPrefix, logPath)

    logger.info { "Retrieving logs from GCS path: $cloudLogPath" }
    val gcsClient: Storage = gcsClientFactory.get()

    logger.info { "Start GCS list and delete request." }
    val blobs = gcsClient.list(storageBucketConfig.log, Storage.BlobListOption.prefix(cloudLogPath))
    blobs.iterateAll().forEach(Consumer { blob: Blob -> blob.delete(Blob.BlobSourceOption.generationMatch()) })
    logger.info { "Finished all deletes." }
  }

  private fun retrieveFiles(
    blobs: List<Blob>,
    numLines: Int,
  ): List<String> {
    val inMemoryData = ByteArrayOutputStream()
    val lines = mutableListOf<String>()
    val lineCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_COUNT_RETRIEVED.metricName,
        logClientType = LogClientType.GCS,
      )
    val byteCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_BYTES_RETRIEVED.metricName,
        logClientType = LogClientType.GCS,
      )

    // iterate through blobs in descending order (oldest first)
    for (blob in blobs) {
      inMemoryData.reset()

      blob.downloadTo(inMemoryData)

      val currFileLines =
        inMemoryData
          .toString(StandardCharsets.UTF_8)
          .split("\n".toRegex())
          .dropLastWhile { it.isEmpty() }
          .toTypedArray()
      // Iterate through the lines in reverse order. This ensures we keep the newer messages over the
      // older messages if we hit the numLines limit.
      for (j in currFileLines.indices.reversed()) {
        val line = currFileLines[j]
        lineCounter?.increment()
        byteCounter?.increment(line.length.toDouble())
        lines.add(line)
        if (lines.size >= numLines) {
          break
        }
      }

      if (lines.size >= numLines) {
        break
      }
    }

    // finally reverse the lines so they're returned in ascending order
    lines.reverse()
    return lines
  }
}

@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^s3$|(?i)^minio$")
@Named("s3LogClient")
class S3LogClient(
  s3LogClientFactory: LogClientFactory<S3Client>,
  private val storageBucketConfig: StorageBucketConfig,
  private val meterRegistry: MeterRegistry?,
  @Value("\${airbyte.logging.client.cloud.log-prefix:job-logging}") private val jobLoggingCloudPrefix: String,
  @Value("\${$STORAGE_TYPE}") private val storageType: String,
) : CloudLogClient {
  companion object {
    private fun getAscendingObjectKeys(
      s3Client: S3Client,
      logPath: String,
      s3Bucket: String,
    ): List<String> {
      val listObjReq =
        ListObjectsV2Request
          .builder()
          .bucket(s3Bucket)
          .prefix(logPath)
          .build()

      val ascendingTimestampObjs: List<String> =
        s3Client
          .listObjectsV2Paginator(listObjReq)
          // Objects are returned in lexicographical order.
          .flatMap { it.contents() }
          .map { it.key() }
          .toList()

      return ascendingTimestampObjs
    }

    @Throws(IOException::class)
    private fun getCurrFile(
      s3Client: S3Client,
      s3Bucket: String,
      poppedKey: String,
    ): List<String> {
      val getObjReq =
        GetObjectRequest
          .builder()
          .key(poppedKey)
          .bucket(s3Bucket)
          .build()

      val data = s3Client.getObjectAsBytes(getObjReq).asByteArray()
      val inputStream = ByteArrayInputStream(data)
      val currentFileLines = mutableListOf<String>()

      BufferedReader(InputStreamReader(inputStream, StandardCharsets.UTF_8)).use { reader ->
        var temp = reader.readLine()
        while (temp != null) {
          currentFileLines.add(temp)
          temp = reader.readLine()
        }
      }
      return currentFileLines
    }
  }

  private var s3Client: S3Client = s3LogClientFactory.get()

  override fun deleteLogs(logPath: String) {
    val cloudLogPath = sanitisePath(jobLoggingCloudPrefix, logPath)
    logger.debug { "Deleting logs from S3 path: $cloudLogPath" }

    val s3Bucket: String = storageBucketConfig.log
    val keys: List<ObjectIdentifier> =
      getAscendingObjectKeys(s3Client, cloudLogPath, s3Bucket)
        .mapNotNull { ObjectIdentifier.builder().key(it).build() }
        .toList()
    val del = Delete.builder().objects(keys).build()

    val multiObjectDeleteRequest =
      DeleteObjectsRequest
        .builder()
        .bucket(s3Bucket)
        .delete(del)
        .build()

    s3Client.deleteObjects(multiObjectDeleteRequest)
    logger.debug { "Multiple objects are deleted!" }
  }

  override fun tailCloudLogs(
    logPath: String,
    numLines: Int,
  ): List<String> {
    val cloudLogPath = sanitisePath(jobLoggingCloudPrefix, logPath)
    logger.debug { "Tailing logs from S3 path: $cloudLogPath" }

    val s3Bucket: String = storageBucketConfig.log
    logger.debug { "Start making S3 list request." }
    val ascendingTimestampKeys: List<String> = getAscendingObjectKeys(s3Client, cloudLogPath, s3Bucket)
    val descendingTimestampKeys = ascendingTimestampKeys.reversed()

    val instrumentedDescendingTimestampKeys =
      meterRegistry.createGauge(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVED.metricName,
        logClientType = LogClientType.valueOf(storageType.uppercase()),
        stateObject = descendingTimestampKeys,
      )
    val timer =
      meterRegistry.createTimer(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVAL_TIME_MS.metricName,
        logClientType = LogClientType.valueOf(storageType.uppercase()),
      )
    val result =
      if (timer != null) {
        timer.recordCallable { retrieveFiles(keys = instrumentedDescendingTimestampKeys, numLines = numLines) }
      } else {
        retrieveFiles(keys = instrumentedDescendingTimestampKeys, numLines = numLines)
      }
    logger.debug { "Done retrieving S3 logs: $cloudLogPath." }
    return result
  }

  private fun retrieveFiles(
    keys: List<String>,
    numLines: Int,
  ): List<String> {
    val lines = mutableListOf<String>()
    var linesRead = 0
    val s3Bucket: String = storageBucketConfig.log
    val lineCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_COUNT_RETRIEVED.metricName,
        logClientType = LogClientType.valueOf(storageType.uppercase()),
      )
    val byteCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_BYTES_RETRIEVED.metricName,
        logClientType = LogClientType.valueOf(storageType.uppercase()),
      )

    val mutatedKeys = keys.toMutableList()

    logger.debug { "Start getting S3 objects." }
    while (linesRead <= numLines && mutatedKeys.isNotEmpty()) {
      val poppedKey = mutatedKeys.removeAt(0)
      val currFileLinesReversed = getCurrFile(s3Client, s3Bucket, poppedKey).reversed()
      for (line in currFileLinesReversed) {
        if (linesRead == numLines) {
          break
        }

        lineCounter?.increment()
        byteCounter?.increment(line.length.toDouble())
        lines.add(0, line)
        linesRead++
      }
    }

    return lines
  }
}

@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^local$")
@Named("localLogClient")
class LocalLogClient : LogClient {
  override fun deleteLogs(logPath: String) {
    Path.of(logPath).deleteExisting()
  }

  override fun tailCloudLogs(
    logPath: String,
    numLines: Int,
  ): List<String> {
    logger.debug { "Tailing logs from S3 path: $logPath" }
    var lineCount = 0
    val lines = mutableListOf<String>()
    val reader = ReversedLinesFileReader(Path.of(logPath).toFile(), Charsets.UTF_8)

    reader.use { r ->
      var moreToRead = true
      while (moreToRead) {
        val line = r.readLine()
        if (line != null && lineCount < numLines) {
          lines.add(line)
        } else {
          moreToRead = false
        }
        lineCount++
      }
    }
    return lines.reversed()
  }
}
