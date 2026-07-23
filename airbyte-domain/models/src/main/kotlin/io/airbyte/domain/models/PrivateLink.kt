/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.OffsetDateTime
import java.util.UUID

enum class CloudProvider {
  AWS,
}

enum class PrivateLinkStatus {
  CREATING,
  PENDING_ACCEPTANCE,
  CONFIGURING,
  AVAILABLE,
  CREATE_FAILED,
  DELETING,
  DELETE_FAILED,
  DELETED,
}

enum class PrivateLinkServiceType {
  ENDPOINT,
  STORAGE,
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  JsonSubTypes.Type(value = PrivateLinkServiceConfig.Endpoint::class, name = "endpoint"),
  JsonSubTypes.Type(value = PrivateLinkServiceConfig.Storage::class, name = "storage"),
)
sealed interface PrivateLinkServiceConfig {
  // Kotlin-side mirror of the JSONB `type` discriminator. Jackson writes the
  // discriminator via the @JsonTypeInfo annotation; this property is JsonIgnored
  // so it isn't also serialized as a regular field.
  @get:JsonIgnore
  val type: PrivateLinkServiceType

  // Shape generation stamp. Matches V2_1_0_024__AddServiceTypeAndServiceConfigToPrivateLink's
  // backfill: every JSONB blob carries `version`, letting readers tell shape generations apart
  // (rolling deploys, DB restores). Bump when the JSONB schema changes incompatibly.
  val version: Int
    get() = 1

  val name: String
  val region: String

  data class Endpoint(
    override val name: String,
    override val region: String,
  ) : PrivateLinkServiceConfig {
    @get:JsonIgnore
    override val type = PrivateLinkServiceType.ENDPOINT
  }

  data class Storage(
    override val region: String,
    val bucket: String,
  ) : PrivateLinkServiceConfig {
    @get:JsonIgnore
    override val type = PrivateLinkServiceType.STORAGE
    override val name: String = "com.amazonaws.$region.s3"
  }
}

data class PrivateLink(
  val id: UUID? = null,
  val workspaceId: UUID,
  val dataplaneGroupId: UUID,
  val name: String,
  val status: PrivateLinkStatus,
  val serviceType: PrivateLinkServiceType,
  val serviceConfig: PrivateLinkServiceConfig,
  val endpointId: String? = null,
  val dnsName: String? = null,
  val scopedConfigurationId: UUID? = null,
  val createdAt: OffsetDateTime? = null,
  val updatedAt: OffsetDateTime? = null,
)
