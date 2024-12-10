package io.airbyte.workers.models

import java.util.UUID

/**
 * A class holding the input to the Temporal schema refresh activity.
 */
data class RefreshSchemaActivityInput(
  // The id of the source catalog associated with this connection.
  val sourceCatalogId: UUID? = null,
// The id of the connection for which we're refreshing the schema.
  val connectionId: UUID? = null,
// The workspace that contains the connection, used mostly for feature flagging.
  val workspaceId: UUID? = null,
)
