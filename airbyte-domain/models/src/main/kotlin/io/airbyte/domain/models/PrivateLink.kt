/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

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

sealed interface PrivateLinkServiceConfig {
  val serviceType: PrivateLinkServiceType
  val name: String
  val region: String

  data class Endpoint(
    override val name: String,
    override val region: String,
  ) : PrivateLinkServiceConfig {
    override val serviceType = PrivateLinkServiceType.ENDPOINT
  }

  data class Storage(
    override val region: String,
    val bucket: String? = null,
  ) : PrivateLinkServiceConfig {
    override val serviceType = PrivateLinkServiceType.STORAGE
    override val name: String = "com.amazonaws.$region.s3"
  }
}

data class PrivateLink(
  val id: UUID? = null,
  val workspaceId: UUID,
  val dataplaneGroupId: UUID,
  val name: String,
  val status: PrivateLinkStatus,
  val serviceRegion: String,
  val serviceName: String,
  val serviceType: PrivateLinkServiceType,
  val serviceConfig: PrivateLinkServiceConfig,
  val endpointId: String? = null,
  val dnsName: String? = null,
  val scopedConfigurationId: UUID? = null,
  val createdAt: OffsetDateTime? = null,
  val updatedAt: OffsetDateTime? = null,
)
