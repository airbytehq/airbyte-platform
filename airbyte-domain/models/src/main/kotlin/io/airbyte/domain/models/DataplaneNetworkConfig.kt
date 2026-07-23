/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.time.OffsetDateTime
import java.util.UUID

sealed interface NetworkConfig {
  data class Aws(
    val region: String,
    val vpcId: String,
    val subnetIds: List<String>,
    val securityGroupIds: List<String>,
  ) : NetworkConfig {
    init {
      require(region.isNotBlank()) { "region must not be blank" }
      require(vpcId.isNotBlank()) { "vpcId must not be blank" }
      require(subnetIds.isNotEmpty()) { "subnetIds must not be empty" }
      require(securityGroupIds.isNotEmpty()) { "securityGroupIds must not be empty" }
    }
  }
}

data class DataplaneNetworkConfig(
  val id: UUID? = null,
  val dataplaneGroupId: UUID,
  val provider: CloudProvider,
  val config: NetworkConfig,
  val createdAt: OffsetDateTime? = null,
  val updatedAt: OffsetDateTime? = null,
)
