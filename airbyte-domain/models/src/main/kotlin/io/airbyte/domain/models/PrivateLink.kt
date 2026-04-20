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

data class PrivateLink(
  val id: UUID? = null,
  val workspaceId: UUID,
  val dataplaneGroupId: UUID,
  val name: String,
  val status: PrivateLinkStatus,
  val serviceRegion: String,
  val serviceName: String,
  val endpointId: String? = null,
  val dnsName: String? = null,
  val scopedConfigurationId: UUID? = null,
  val createdAt: OffsetDateTime? = null,
  val updatedAt: OffsetDateTime? = null,
)
