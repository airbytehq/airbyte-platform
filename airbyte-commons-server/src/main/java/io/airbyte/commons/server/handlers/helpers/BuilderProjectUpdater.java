/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.config.persistence.ConfigNotFoundException;
import java.io.IOException;

public interface BuilderProjectUpdater {

  void persistBuilderProjectUpdate(final ExistingConnectorBuilderProjectWithWorkspaceId projectUpdate) throws ConfigNotFoundException, IOException;

}
