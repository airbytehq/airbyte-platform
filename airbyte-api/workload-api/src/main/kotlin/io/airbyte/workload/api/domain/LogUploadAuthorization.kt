/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.OffsetDateTime

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.EXISTING_PROPERTY,
  property = "type",
)
@JsonSubTypes(
  JsonSubTypes.Type(
    value = GcsDownscopedOAuthLogUploadAuthorization::class,
    name = "GCS_DOWNSCOPED_OAUTH",
  ),
)
sealed interface LogUploadAuthorization {
  val type: LogUploadAuthorizationType
}

enum class LogUploadAuthorizationType {
  GCS_DOWNSCOPED_OAUTH,
}

data class GcsDownscopedOAuthLogUploadAuthorization(
  val accessToken: String,
  @field:JsonFormat(shape = JsonFormat.Shape.STRING)
  val expiresAt: OffsetDateTime,
  val bucketName: String,
  val objectKeyPrefix: String,
) : LogUploadAuthorization {
  override val type: LogUploadAuthorizationType = LogUploadAuthorizationType.GCS_DOWNSCOPED_OAUTH

  override fun toString(): String =
    "GcsDownscopedOAuthLogUploadAuthorization(accessToken=******, expiresAt=$expiresAt, bucketName=******, objectKeyPrefix=******, type=$type)"
}
