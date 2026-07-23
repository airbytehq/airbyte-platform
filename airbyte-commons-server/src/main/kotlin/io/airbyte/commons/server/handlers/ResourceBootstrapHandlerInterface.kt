/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceRead

interface ResourceBootstrapHandlerInterface {
  fun bootStrapWorkspaceForCurrentUser(workspaceCreateWithId: WorkspaceCreateWithId): WorkspaceRead
}
