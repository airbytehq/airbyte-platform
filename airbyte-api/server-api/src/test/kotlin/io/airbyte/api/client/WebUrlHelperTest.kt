/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import io.airbyte.micronaut.runtime.AirbyteConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class WebUrlHelperTest {
  private val airbyteUrl: String = "https://cloud.airbyte.com"
  private val airbyteAgentsUrl: String = "https://app.airbyte.ai"
  private val connectionId: UUID = UUID.randomUUID()
  private val destinationId: UUID = UUID.randomUUID()
  private val sourceId: UUID = UUID.randomUUID()
  private val workspaceId: UUID = UUID.randomUUID()

  private lateinit var airbyteConfig: AirbyteConfig
  private lateinit var webUrlHelper: WebUrlHelper

  @BeforeEach
  fun setup() {
    airbyteConfig = AirbyteConfig(airbyteUrl = airbyteUrl, airbyteAgentsUrl = airbyteAgentsUrl)
    webUrlHelper = WebUrlHelper(airbyteConfig)
  }

  @Test
  fun testGetBaseUrl() {
    assertEquals(airbyteUrl, webUrlHelper.baseUrl)
  }

  @Test
  fun testGetAgentsBaseUrl() {
    assertEquals(airbyteAgentsUrl, webUrlHelper.agentsBaseUrl)
  }

  @Test
  fun testAgentsBaseUrlFallsBackToBaseUrlWhenUnset() {
    val helper = WebUrlHelper(AirbyteConfig(airbyteUrl = airbyteUrl))
    assertEquals(airbyteUrl, helper.agentsBaseUrl)
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
