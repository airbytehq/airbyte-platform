/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.data.ConfigNotFoundException
import java.io.IOException

interface BuilderProjectUpdater {
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun persistBuilderProjectUpdate(projectUpdate: ExistingConnectorBuilderProjectWithWorkspaceId)
}
