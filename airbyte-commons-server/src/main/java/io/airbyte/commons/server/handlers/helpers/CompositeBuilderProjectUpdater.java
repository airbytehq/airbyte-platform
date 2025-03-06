/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import java.io.IOException;
import java.util.List;

public class CompositeBuilderProjectUpdater implements BuilderProjectUpdater {
  /*
   * Update multiple builder project updaters sequentially. The update method is intentionally not
   * atomic as this is an experimental features. We don't want a problematic updater to prevent others
   * from succeeding. This means it is possible for them to get out of sync.
   */

  private final List<BuilderProjectUpdater> updaters;

  public CompositeBuilderProjectUpdater(final List<BuilderProjectUpdater> updaters) {
    this.updaters = updaters;
  }

  @Override
  public void persistBuilderProjectUpdate(final ExistingConnectorBuilderProjectWithWorkspaceId projectUpdate)
      throws IOException, ConfigNotFoundException {
    for (final BuilderProjectUpdater updater : updaters) {
      updater.persistBuilderProjectUpdate(projectUpdate);
    }
  }

}
