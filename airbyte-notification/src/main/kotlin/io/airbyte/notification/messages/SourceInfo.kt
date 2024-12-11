package io.airbyte.notification.messages

import java.util.UUID

data class SourceInfo(
  val id: UUID?,
  val name: String?,
  val url: String?,
)
