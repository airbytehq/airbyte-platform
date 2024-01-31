/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.messages;

import io.airbyte.api.model.generated.CatalogDiff;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class SchemaUpdateNotification {

  private WorkspaceInfo workspace;

  private ConnectionInfo connectionInfo;

  private SourceInfo sourceInfo;

  private boolean isBreakingChange;

  private CatalogDiff catalogDiff;

}
