/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceRead
import org.jooq.DSLContext

interface ResourceBootstrapHandlerInterface {
  fun bootStrapWorkspaceForCurrentUser(workspaceCreateWithId: WorkspaceCreateWithId): WorkspaceRead

  fun bootStrapWorkspaceForCurrentUser(
    ctx: DSLContext,
    workspaceCreateWithId: WorkspaceCreateWithId,
  ): WorkspaceRead = bootStrapWorkspaceForCurrentUser(workspaceCreateWithId)
}
