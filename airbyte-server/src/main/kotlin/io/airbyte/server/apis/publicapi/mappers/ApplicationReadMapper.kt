/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.Application
import io.airbyte.publicApi.server.generated.models.ApplicationRead
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

fun toApplicationRead(application: Application): ApplicationRead =
  ApplicationRead(
    id = application.id,
    name = application.name,
    clientId = application.clientId,
    clientSecret = application.clientSecret,
    createdAt =
      OffsetDateTime
        .parse(application.createdOn, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toEpochSecond(),
  )
