/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonIgnore
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType

interface ConnectionEvent {
  // These enums are also defined in openapi config.yaml, please maintain the consistency between them.
  // If any change made in one place, please do the same in the other place.
  @TypeDef(type = DataType.STRING)
  enum class Type {
    SYNC_STARTED, // only for manual sync jobs
    SYNC_SUCCEEDED,
    SYNC_INCOMPLETE,
    SYNC_FAILED,
    SYNC_CANCELLED,
    REFRESH_STARTED,
    REFRESH_SUCCEEDED,
    REFRESH_INCOMPLETE,
    REFRESH_FAILED,
    REFRESH_CANCELLED,
    CLEAR_STARTED,
    CLEAR_SUCCEEDED,
    CLEAR_INCOMPLETE,
    CLEAR_FAILED,
    CLEAR_CANCELLED,
    CONNECTION_SETTINGS_UPDATE,
    CONNECTION_ENABLED,
    CONNECTION_DISABLED,
    SCHEMA_UPDATE,
    SCHEMA_CONFIG_UPDATE,
    CONNECTOR_UPDATE,
    UNKNOWN,
  }

  @JsonIgnore
  fun getEventType(): Type
}
