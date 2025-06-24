/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.FailureReason
import io.airbyte.notification.messages.ConnectionInfo
import io.airbyte.notification.messages.DestinationInfo
import io.airbyte.notification.messages.SourceInfo
import io.airbyte.notification.messages.SyncSummary
import io.airbyte.notification.messages.WorkspaceInfo
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

internal class NotificationTest {
  @Test
  @Throws(JsonProcessingException::class)
  fun testTextSerialization() {
    val notification = Notification()
    notification.setText("test content")

    val mapper = ObjectMapper()

    val expected =
      mapper.readTree(
        """
        { "text": "test content" }
        """.trimIndent(),
      )
    Assertions.assertEquals(expected, notification.toJsonNode())
  }

  @Test
  @Throws(JsonProcessingException::class)
  fun testBlockSerialization() {
    val notification = Notification()
    notification.setText("A text node")

    val section = notification.addSection()
    section.setText("Section text")
    notification.addDivider()
    val anotherSection = notification.addSection()
    val field1 = anotherSection.addField()
    field1.setText("field1")
    field1.setType("text")
    val field2 = anotherSection.addField()
    field2.setText("another field")
    field2.setType("mkdwn")

    val mapper = ObjectMapper()

    val expected =
      mapper.readTree(
        """
        {
          "text": "A text node",
          "blocks": [
            {
              "type": "section",
              "text": {
                "type": "mrkdwn",
                "text": "Section text"
              }
            },
            {
              "type": "divider"
            },
            {
              "type": "section",
              "fields": [
                {
                  "type": "text",
                  "text": "field1"
                },
                {
                  "type": "mkdwn",
                  "text": "another field"
                }
              ]
            }
          ]
        }
        """.trimIndent(),
      )
    Assertions.assertEquals(expected, notification.toJsonNode())
  }

  @Test
  @Throws(JsonProcessingException::class)
  fun testDataNode() {
    val workspaceId = UUID.fromString("b510e39b-e9e2-4833-9a3a-963e51d35fb4")
    val connectionId = UUID.fromString("64d901a1-2520-4d91-93c8-9df438668ff0")
    val sourceId = UUID.fromString("c0655b08-1511-4e72-b7da-24c5d54de532")
    val destinationId = UUID.fromString("5621c38f-8048-4abb-85ca-b34ff8d9a298")
    val jobId = 9988L

    val syncSummary =
      SyncSummary(
        WorkspaceInfo(workspaceId, "Workspace1", "https://link/to/ws"),
        ConnectionInfo(connectionId, "Connection", "https://link/to/connection"),
        SourceInfo(sourceId, "Source", "https://link/to/source"),
        DestinationInfo(destinationId, "Destination", "https://link/to/destination"),
        jobId,
        false,
        Instant.ofEpochSecond(1704067200),
        Instant.ofEpochSecond(1704070800),
        1000L,
        90L,
        89L,
        45L,
        0,
        0,
        "Something failed",
        FailureReason.FailureType.CONFIG_ERROR,
        FailureReason.FailureOrigin.DESTINATION,
      )
    val notification = Notification()
    notification.setData(syncSummary)

    val mapper = ObjectMapper()
    mapper.enable(DeserializationFeature.USE_LONG_FOR_INTS)

    val expected =
      mapper.readTree(
        """
        {
          "workspace": {
            "id":"b510e39b-e9e2-4833-9a3a-963e51d35fb4",
            "name":"Workspace1",
            "url":"https://link/to/ws"
          },
          "connection":{
            "id":"64d901a1-2520-4d91-93c8-9df438668ff0",
            "name":"Connection",
            "url":"https://link/to/connection"
          },
          "source":{
            "id":"c0655b08-1511-4e72-b7da-24c5d54de532",
            "name":"Source",
            "url":"https://link/to/source"
          },
          "destination":{
            "id":"5621c38f-8048-4abb-85ca-b34ff8d9a298",
            "name":"Destination",
            "url":"https://link/to/destination"
          },
          "jobId":9988,
          "startedAt":"2024-01-01T00:00:00Z",
          "finishedAt":"2024-01-01T01:00:00Z",
          "bytesEmitted":1000,
          "bytesCommitted":90,
          "recordsEmitted":89,
          "recordsFilteredOut":0,
          "bytesFilteredOut":0,
          "recordsCommitted":45,
          "errorMessage":"Something failed",
          "errorType":"config_error",
          "errorOrigin":"destination",
          "bytesEmittedFormatted": "1000 B",
          "bytesCommittedFormatted":"90 B",
          "success":false,
          "durationInSeconds":3600,
          "durationFormatted":"1 hours 0 min"
        }
        """.trimIndent(),
      )
    val notificationJson = notification.toJsonNode()
    Assertions.assertTrue(notificationJson.has("data"))
    Assertions.assertEquals(expected, notificationJson["data"])
  }
}
