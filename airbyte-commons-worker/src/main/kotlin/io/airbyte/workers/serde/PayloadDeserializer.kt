/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.serde

import io.airbyte.commons.json.Jsons
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import jakarta.inject.Singleton

@Singleton
class PayloadDeserializer {
  fun toReplicationActivityInput(payload: String): ReplicationActivityInput = Jsons.deserialize(payload, ReplicationActivityInput::class.java)

  fun toCheckConnectionInput(payload: String): CheckConnectionInput = Jsons.deserialize(payload, CheckConnectionInput::class.java)

  fun toDiscoverCatalogInput(payload: String): DiscoverCatalogInput = Jsons.deserialize(payload, DiscoverCatalogInput::class.java)

  fun toSpecInput(payload: String): SpecInput = Jsons.deserialize(payload, SpecInput::class.java)
}
