package io.airbyte.data.services.shared

import java.util.UUID

interface ConnectionEvent {
  enum class Type {
    SYNC_SUCCEEDED,
  }

  fun getUserId(): UUID? {
    return null
  }

  fun getEventType(): Type
}
