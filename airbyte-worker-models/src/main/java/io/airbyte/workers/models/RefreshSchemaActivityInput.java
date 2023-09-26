/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A class holding the input to the Temporal schema refresh activity.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RefreshSchemaActivityInput {

  // The id of the source catalog associated with this connection.
  private UUID sourceCatalogId;
  // The id of the connection for which we're refreshing the schema.
  private UUID connectionId;
  // The workspace that contains the connection, used mostly for feature flagging.
  private UUID workspaceId;

}
