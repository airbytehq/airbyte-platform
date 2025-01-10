package io.airbyte.notification.messages

import java.util.UUID

data class DestinationInfo(
  val id: UUID?,
  val name: String?,
  val url: String?,
)
