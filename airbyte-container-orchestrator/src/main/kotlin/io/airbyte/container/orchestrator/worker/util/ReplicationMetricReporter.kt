/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import jakarta.inject.Singleton
import javax.annotation.PostConstruct

@Singleton
class ReplicationMetricReporter(
  private val metricClient: MetricClient,
  val replicationInput: ReplicationInput,
) {
  private val dockerImage = replicationInput.sourceLauncherConfig.dockerImage
  private lateinit var dockerRepo: String
  private lateinit var dockerVersion: String

  @PostConstruct
  fun initialize() {
    dockerRepo = dockerImage.split(":").first()
    dockerVersion = dockerImage.split(":").last()
  }

  /**
   * Given a [AirbyteStreamNameNamespacePair] and a String Set of validationErrors, produce a DataDog
   * count metric for each error.
   */
  fun trackSchemaValidationErrors(
    stream: AirbyteStreamNameNamespacePair?,
    validationErrors: MutableSet<String?>?,
  ) {
    // Create a copy of the validationErrors set to prevent ConcurrentModificationExceptions
    // that can occur while iterating over the set.
    val copiedErrors = setOf(validationErrors)
    val attributes: MutableList<MetricAttribute> =
      copiedErrors
        .mapNotNull { f ->
          MetricAttribute("validation_error", f.toString().replace(",", ""))
        }.toList()
        .toMutableList()
    attributes.add(MetricAttribute("docker_repo", dockerRepo))
    attributes.add(MetricAttribute("docker_version", dockerVersion))
    attributes.add(MetricAttribute("stream", stream.toString()))
    metricClient.count(
      metric = OssMetricsRegistry.NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS,
      value = validationErrors?.size?.toLong() ?: 0L,
      attributes = attributes.toTypedArray(),
    )
  }

  /**
   * Given a [AirbyteStreamNameNamespacePair] and a String Set of unexpectedFieldNames, produce a
   * DataDog count metric for each unexpected field.
   */
  fun trackUnexpectedFields(
    stream: AirbyteStreamNameNamespacePair,
    unexpectedFieldNames: MutableSet<String>,
  ) {
    // Create a copy of the unexpectedFieldNames set to prevent ConcurrentModificationExceptions
    // that can occur while iterating over the set.
    val copiedUnexpected = setOf(unexpectedFieldNames)
    val attributes: MutableList<MetricAttribute> =
      copiedUnexpected
        .stream()
        .map { f -> MetricAttribute("field_name", f.toString()) }
        .toList()
        .toMutableList()
    attributes.add(MetricAttribute("docker_repo", dockerRepo))
    attributes.add(MetricAttribute("docker_version", dockerVersion))
    attributes.add(MetricAttribute("stream", stream.toString()))
    val attributesArr = attributes.toTypedArray<MetricAttribute>()
    metricClient.count(
      metric = OssMetricsRegistry.NUM_UNEXPECTED_FIELDS_IN_STREAMS,
      value = unexpectedFieldNames.size.toLong(),
      attributes = attributesArr,
    )
  }
}
