/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.AuthRefreshToken
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias EntityAuthRefreshToken = AuthRefreshToken
typealias ModelAuthRefreshToken = io.airbyte.config.AuthRefreshToken

fun EntityAuthRefreshToken.toConfigModel(): ModelAuthRefreshToken =
  ModelAuthRefreshToken()
    .withValue(this.value)
    .withSessionId(this.sessionId)
    .withRevoked(this.revoked)
    .withCreatedAt(this.createdAt?.toEpochSecond())
    .withUpdatedAt(this.updatedAt?.toEpochSecond())

fun ModelAuthRefreshToken.toEntity(): EntityAuthRefreshToken =
  EntityAuthRefreshToken(
    value = this.value,
    sessionId = this.sessionId,
    revoked = this.revoked,
    createdAt = this.createdAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    updatedAt = this.updatedAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
  )
