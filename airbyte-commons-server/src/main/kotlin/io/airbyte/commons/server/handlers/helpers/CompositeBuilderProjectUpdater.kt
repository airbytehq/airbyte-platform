/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.data.ConfigNotFoundException
import java.io.IOException

  /*
   * Update multiple builder project updaters sequentially. The update method is intentionally not
   * atomic as this is an experimental features. We don't want a problematic updater to prevent others
   * from succeeding. This means it is possible for them to get out of sync.
   */
class CompositeBuilderProjectUpdater(
  private val updaters: List<BuilderProjectUpdater>,
) : BuilderProjectUpdater {
  @Throws(IOException::class, ConfigNotFoundException::class)
  override fun persistBuilderProjectUpdate(projectUpdate: ExistingConnectorBuilderProjectWithWorkspaceId) {
    for (updater in updaters) {
      updater.persistBuilderProjectUpdate(projectUpdate)
    }
  }
}
