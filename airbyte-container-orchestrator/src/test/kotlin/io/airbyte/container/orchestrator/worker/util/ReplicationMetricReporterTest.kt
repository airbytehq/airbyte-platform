/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

private const val DOCKER_IMAGE = "test/image:1.2.3"
private const val NAME = "name"
private const val NAMESPACE = "namespace"

internal class ReplicationMetricReporterTest {
  @Test
  fun testTrackingSchemaValidationErrors() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val validationErrors: MutableSet<String?>? = mutableSetOf("error1", "error2")
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
      }
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk()
      }
    val replicationInput =
      mockk<ReplicationInput> {
        every { sourceLauncherConfig } returns srcLauncherConfig
      }

    val reporter = ReplicationMetricReporter(metricClient = metricClient, replicationInput = replicationInput)
    reporter.initialize()

    reporter.trackSchemaValidationErrors(stream = stream, validationErrors = validationErrors)

    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS,
        value = validationErrors!!.size.toLong(),
        attributes = anyVararg(),
      )
    }
  }

  @Test
  fun testTrackingSchemaValidationErrorsEmptySet() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val validationErrors: MutableSet<String?>? = mutableSetOf()
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
      }
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk()
      }
    val replicationInput =
      mockk<ReplicationInput> {
        every { sourceLauncherConfig } returns srcLauncherConfig
      }

    val reporter = ReplicationMetricReporter(metricClient = metricClient, replicationInput = replicationInput)
    reporter.initialize()

    reporter.trackSchemaValidationErrors(stream = stream, validationErrors = validationErrors)

    verify(exactly = 1) {
      metricClient.count(metric = OssMetricsRegistry.NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS, value = 0L, attributes = anyVararg())
    }
  }

  @Test
  fun testTrackingSchemaValidationErrorsNullSet() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val validationErrors: MutableSet<String?>? = null
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
      }
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk()
      }
    val replicationInput =
      mockk<ReplicationInput> {
        every { sourceLauncherConfig } returns srcLauncherConfig
      }

    val reporter = ReplicationMetricReporter(metricClient = metricClient, replicationInput = replicationInput)
    reporter.initialize()

    reporter.trackSchemaValidationErrors(stream = stream, validationErrors = validationErrors)

    verify(exactly = 1) {
      metricClient.count(metric = OssMetricsRegistry.NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS, value = 0L, attributes = anyVararg())
    }
  }

  @Test
  fun testTrackUnexpectedFields() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val unexpectedFieldNames = mutableSetOf("field1", "field2")
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
      }
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk()
      }
    val replicationInput =
      mockk<ReplicationInput> {
        every { sourceLauncherConfig } returns srcLauncherConfig
      }

    val reporter = ReplicationMetricReporter(metricClient = metricClient, replicationInput = replicationInput)
    reporter.initialize()

    reporter.trackUnexpectedFields(stream = stream, unexpectedFieldNames = unexpectedFieldNames)

    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.NUM_UNEXPECTED_FIELDS_IN_STREAMS,
        value = unexpectedFieldNames.size.toLong(),
        attributes = anyVararg(),
      )
    }
  }

  @Test
  fun testTrackUnexpectedFieldsEmptySet() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val unexpectedFieldNames = mutableSetOf<String>()
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
      }
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk()
      }
    val replicationInput =
      mockk<ReplicationInput> {
        every { sourceLauncherConfig } returns srcLauncherConfig
      }

    val reporter = ReplicationMetricReporter(metricClient = metricClient, replicationInput = replicationInput)
    reporter.initialize()

    reporter.trackUnexpectedFields(stream = stream, unexpectedFieldNames = unexpectedFieldNames)

    verify(exactly = 1) { metricClient.count(metric = OssMetricsRegistry.NUM_UNEXPECTED_FIELDS_IN_STREAMS, value = 0L, attributes = anyVararg()) }
  }
}
