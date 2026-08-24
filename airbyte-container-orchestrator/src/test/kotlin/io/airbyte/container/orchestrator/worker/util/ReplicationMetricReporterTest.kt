/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

private const val DOCKER_IMAGE = "test/image:1.2.3"
private const val NAME = "name"
private const val NAMESPACE = "namespace"
private const val CONNECTOR_NAME = "Connector Name"
private const val CONNECTOR_NAME_WITH_COMMA = "Connector, Name"

internal class ReplicationMetricReporterTest {
  @Test
  fun testTrackingSchemaValidationErrors() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val validationErrors: MutableSet<String?>? = mutableSetOf("error1", "error2")
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
        every { connectorDefinitionName } returns CONNECTOR_NAME_WITH_COMMA
      }
    val capturedAttributes = mutableListOf<List<MetricAttribute>>()
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = any(), value = any(), attributes = anyVararg()) } answers {
          @Suppress("UNCHECKED_CAST")
          capturedAttributes += (args[2] as Array<MetricAttribute?>).filterNotNull()
          null
        }
      }
    val replicationInput =
      mockk<ReplicationInput> {
        every { sourceLauncherConfig } returns srcLauncherConfig
      }

    val reporter = ReplicationMetricReporter(metricClient = metricClient, replicationInput = replicationInput)
    reporter.initialize()

    reporter.trackSchemaValidationErrors(stream = stream, validationErrors = validationErrors)

    assertEquals(
      setOf("error1", "error2"),
      capturedAttributes
        .single()
        .filter { it.key == "validation_error" }
        .map { it.value }
        .toSet(),
    )
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS,
        value = validationErrors!!.size.toLong(),
        attributes = anyVararg(),
      )
    }
    assertThat(capturedAttributes.single()).contains(MetricAttribute("connector", CONNECTOR_NAME_WITH_COMMA.replace(",", "")))
  }

  @Test
  fun testTrackingSchemaValidationErrorsEmptySet() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val validationErrors: MutableSet<String?>? = mutableSetOf()
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
        every { connectorDefinitionName } returns null
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
  fun testTrackingSchemaValidationErrorsFallsBackToDockerRepo() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
        every { connectorDefinitionName } returns null
      }
    val capturedAttributes = mutableListOf<List<MetricAttribute>>()
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = any(), value = any(), attributes = anyVararg()) } answers {
          @Suppress("UNCHECKED_CAST")
          capturedAttributes += (args[2] as Array<MetricAttribute?>).filterNotNull()
          null
        }
      }
    val replicationInput =
      mockk<ReplicationInput> {
        every { sourceLauncherConfig } returns srcLauncherConfig
      }

    val reporter = ReplicationMetricReporter(metricClient = metricClient, replicationInput = replicationInput)
    reporter.initialize()

    reporter.trackSchemaValidationErrors(stream = stream, validationErrors = mutableSetOf("error"))

    assertThat(capturedAttributes.single()).contains(MetricAttribute("connector", "test/image"))
  }

  @Test
  fun testTrackingSchemaValidationErrorsNullSet() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val validationErrors: MutableSet<String?>? = null
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
        every { connectorDefinitionName } returns null
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
        every { connectorDefinitionName } returns CONNECTOR_NAME
      }
    val capturedAttributes = mutableListOf<List<MetricAttribute>>()
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = any(), value = any(), attributes = anyVararg()) } answers {
          @Suppress("UNCHECKED_CAST")
          capturedAttributes += (args[2] as Array<MetricAttribute?>).filterNotNull()
          null
        }
      }
    val replicationInput =
      mockk<ReplicationInput> {
        every { sourceLauncherConfig } returns srcLauncherConfig
      }

    val reporter = ReplicationMetricReporter(metricClient = metricClient, replicationInput = replicationInput)
    reporter.initialize()

    reporter.trackUnexpectedFields(stream = stream, unexpectedFieldNames = unexpectedFieldNames)

    assertEquals(
      setOf("field1", "field2"),
      capturedAttributes
        .single()
        .filter { it.key == "field_name" }
        .map { it.value }
        .toSet(),
    )
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.NUM_UNEXPECTED_FIELDS_IN_STREAMS,
        value = unexpectedFieldNames.size.toLong(),
        attributes = anyVararg(),
      )
    }
    assertThat(capturedAttributes.single()).contains(MetricAttribute("connector", CONNECTOR_NAME))
  }

  @Test
  fun testTrackUnexpectedFieldsEmptySet() {
    val stream = AirbyteStreamNameNamespacePair(NAME, NAMESPACE)
    val unexpectedFieldNames = mutableSetOf<String>()
    val srcLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns DOCKER_IMAGE
        every { connectorDefinitionName } returns null
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
