/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.FromJson
import com.squareup.moshi.ToJson
import io.airbyte.api.client.model.generated.SyncMode

/**
 * Custom Moshi adapter that handles the use of [SyncMode].
 * <p />
 * This adapter ensures that the enumeration value serialized or
 * deserialized by Moshi can be converted to the server-side model representations.
 * This custom adapter is necessary because the server-side model representation
 * is still generated using Java, which expects the enum value to be in lowercase,
 * whereas Moshi by default will serialize the enumerated value by its name, which is in uppercase.
 */
class SyncModeAdapter {
  @ToJson
  fun toJson(value: SyncMode): String {
    return value.value.lowercase()
  }

  @FromJson
  fun fromJson(value: String): SyncMode {
    return SyncMode.decode(value)!!
  }
}
