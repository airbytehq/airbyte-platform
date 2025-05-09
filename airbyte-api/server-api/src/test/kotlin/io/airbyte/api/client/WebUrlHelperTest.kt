/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class WebUrlHelperTest {
  private val airbyteUrl: String = "https://cloud.airbyte.com"
  private val connectionId: UUID = UUID.randomUUID()
  private val destinationId: UUID = UUID.randomUUID()
  private val sourceId: UUID = UUID.randomUUID()
  private val workspaceId: UUID = UUID.randomUUID()

  private lateinit var webUrlHelper: WebUrlHelper

  @BeforeEach
  fun setup() {
    webUrlHelper = WebUrlHelper(airbyteUrl)
  }

  @Test
  fun testGetBaseUrl() {
    assertEquals(airbyteUrl, webUrlHelper.baseUrl)
  }

  @Test
  fun testGetWorkspaceUrl() {
    val workspaceUrl: String = webUrlHelper.getWorkspaceUrl(workspaceId)
    assertEquals("$airbyteUrl/workspaces/$workspaceId", workspaceUrl)
  }

  @Test
  fun testGetConnectionUrl() {
    val connectionUrl: String = webUrlHelper.getConnectionUrl(workspaceId, connectionId)
    assertEquals("$airbyteUrl/workspaces/$workspaceId/connections/$connectionId", connectionUrl)
  }

  @Test
  fun testGetSourceUrl() {
    val sourceUrl: String = webUrlHelper.getSourceUrl(workspaceId, sourceId)
    assertEquals("$airbyteUrl/workspaces/$workspaceId/source/$sourceId", sourceUrl)
  }

  @Test
  fun testGetDestinationUrl() {
    val destinationUrl: String = webUrlHelper.getDestinationUrl(workspaceId, destinationId)
    assertEquals("$airbyteUrl/workspaces/$workspaceId/destination/$destinationId", destinationUrl)
  }

  @Test
  fun testGetConnectionReplicationUrl() {
    val connectionReplicationPageUrl: String = webUrlHelper.getConnectionReplicationPageUrl(workspaceId, connectionId)
    assertEquals("$airbyteUrl/workspaces/$workspaceId/connections/$connectionId/replication", connectionReplicationPageUrl)
  }
}
