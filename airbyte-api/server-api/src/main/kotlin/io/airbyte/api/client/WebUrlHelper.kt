/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Generate URLs for different resources. Abstracts away having to think about the root of the URL
 * which can be different for different OSS and cloud deployments.
 *
 * Depends on the `AIRBYTE_URL` environment variable being present.
 */
@Singleton
@Requires(property = "airbyte.airbyte-url")
class WebUrlHelper(
  @Value("\${airbyte.airbyte-url}") airbyteUrl: String,
) {
  // Original Java-based class exposed the base url, so preserving that functionality here
  val baseUrl: String = airbyteUrl.trimEnd('/')

  /**
   * Get the url for a workspace.
   *
   * @param workspaceId workspace id
   * @return url for the workspace
   */
  fun getWorkspaceUrl(workspaceId: UUID): String = "$baseUrl/workspaces/$workspaceId"

  /**
   * Get the url for a connection.
   *
   * @param workspaceId workspace id
   * @param connectionId connection id
   * @return url for the connection
   */
  fun getConnectionUrl(
    workspaceId: UUID,
    connectionId: UUID,
  ): String = "${getWorkspaceUrl(workspaceId)}/connections/$connectionId"

  /**
   * Get the url for a source.
   *
   * @param workspaceId workspace id
   * @param sourceId source id
   * @return url for the source
   */
  fun getSourceUrl(
    workspaceId: UUID,
    sourceId: UUID,
  ): String = "${getWorkspaceUrl(workspaceId)}/source/$sourceId"

  /**
   * Get the url for a destination.
   *
   * @param workspaceId workspace id
   * @param destinationId destination id
   * @return url for the destination
   */
  fun getDestinationUrl(
    workspaceId: UUID,
    destinationId: UUID,
  ): String = "${getWorkspaceUrl(workspaceId)}/destination/$destinationId"

  /**
   * Get the url for the connection replication web app page.
   *
   * @param workspaceId workspace id
   * @param connectionId connection id
   * @return url for the connection replication page
   */
  fun getConnectionReplicationPageUrl(
    workspaceId: UUID,
    connectionId: UUID,
  ): String = "${getWorkspaceUrl(workspaceId)}/connections/$connectionId/replication"
}
