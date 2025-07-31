/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.utils

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.exception.SizeLimitException
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.github.oshai.kotlinlogging.KotlinLogging

/**
 * Provide validation to detect temporal failures earlier.
 *
 *
 * For example, when an activity returns a result that exceeds temporal payload limit, we may report
 * the activity as a success while it may fail further down in the temporal pipeline. The downside
 * is that having this fail in temporal means that we are mistakenly reporting the activity as
 * successful.
 */
class PayloadChecker(
  private val metricClient: MetricClient,
) {
  /**
   * Validate the payload size fits within temporal message size limits.
   *
   * @param data to validate
   * @param <T> type of data
   * @return data if size is valid
   * @throws SizeLimitException if payload size exceeds temporal limits.
   </T> */
  fun <T> validatePayloadSize(data: T): T = validatePayloadSize(data, emptyArray())

  /**
   * Validate the payload size fits within temporal message size limits.
   *
   * @param data to validate
   * @param <T> type of data
   * @param attrs for metric reporting
   * @return data if size is valid
   * @throws SizeLimitException if payload size exceeds temporal limits.
   </T> */
  fun <T> validatePayloadSize(
    data: T,
    attrs: Array<MetricAttribute>,
  ): T {
    val serializedData = Jsons.serialize(data)
    if (serializedData.length > MAX_PAYLOAD_SIZE_BYTES) {
      emitInspectionLog(data)
      metricClient.count(metric = OssMetricsRegistry.PAYLOAD_SIZE_EXCEEDED, attributes = attrs)
      throw SizeLimitException(String.format("Complete result exceeds size limit (%s of %s)", serializedData.length, MAX_PAYLOAD_SIZE_BYTES))
    }
    return data
  }

  private fun <T> emitInspectionLog(data: T) {
    val jsonData = Jsons.jsonNode(data)
    val inspectionMap: MutableMap<String, Int> = HashMap()
    val it = jsonData.fieldNames()
    while (it.hasNext()) {
      val fieldName = it.next()
      inspectionMap[fieldName] =
        Jsons.serialize(jsonData[fieldName]).length
    }
    log.info("PayloadSize exceeded for object: {}", Jsons.serialize<Map<String, Int>>(inspectionMap))
  }

  companion object {
    private val log = KotlinLogging.logger {}

    const val MAX_PAYLOAD_SIZE_BYTES: Int = 4 * 1024 * 1024
  }
}
