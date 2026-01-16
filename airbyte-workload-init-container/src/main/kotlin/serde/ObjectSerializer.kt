/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.serde

import io.airbyte.commons.json.Jsons
import jakarta.inject.Singleton

@Singleton
class ObjectSerializer {
  fun <T> serialize(config: T): String = Jsons.serialize(config)
}
