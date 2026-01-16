/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.StreamDescriptor
import java.util.UUID

interface StreamStatusesService {
  fun getLastJobIdWithStatsByStream(connectionId: UUID): Map<StreamDescriptor, Long>
}
