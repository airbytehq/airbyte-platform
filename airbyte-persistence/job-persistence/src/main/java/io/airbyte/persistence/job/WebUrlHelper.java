/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import java.util.UUID;

/**
 * Generate URLs for different resources. Abstracts away having to think about the root of the URL
 * which can be different for different OSS and cloud deployments.
 */
public class WebUrlHelper {

  private final String webAppUrl;

  public WebUrlHelper(final String webAppUrl) {
    this.webAppUrl = webAppUrl;
  }

  /**
   * Get url of the webapp.
   *
   * @return url of the webapp (root)
   */
  public String getBaseUrl() {
    if (webAppUrl.endsWith("/")) {
      return webAppUrl.substring(0, webAppUrl.length() - 1);
    }

    return webAppUrl;
  }

  /**
   * Get the url for a workspace.
   *
   * @param workspaceId workspace id
   * @return url for the workspace
   */
  public String getWorkspaceUrl(final UUID workspaceId) {
    return String.format("%s/workspaces/%s", getBaseUrl(), workspaceId);
  }

  /**
   * Get the url for a connection.
   *
   * @param workspaceId workspace id
   * @param connectionId connection id
   * @return url for the connection
   */
  public String getConnectionUrl(final UUID workspaceId, final UUID connectionId) {
    return String.format("%s/connections/%s", getWorkspaceUrl(workspaceId), connectionId);
  }

}
