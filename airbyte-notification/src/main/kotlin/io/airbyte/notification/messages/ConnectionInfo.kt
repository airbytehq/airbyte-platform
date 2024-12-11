package io.airbyte.notification.messages

import java.util.UUID

data class ConnectionInfo(
  val id: UUID? = null,
  val name: String? = null,
  val url: String? = null,
)
