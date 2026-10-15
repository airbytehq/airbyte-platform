/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.server.generated.models.ExistingConnectorBuilderProjectWithWorkspaceId

interface BuilderProjectUpdater {
  fun persistBuilderProjectUpdate(projectUpdate: ExistingConnectorBuilderProjectWithWorkspaceId)
}
