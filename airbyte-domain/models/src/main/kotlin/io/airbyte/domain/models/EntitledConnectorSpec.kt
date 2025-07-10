/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import io.airbyte.protocol.models.v0.ConnectorSpecification

data class EntitledConnectorSpec(
  val spec: ConnectorSpecification,
  val missingEntitlements: List<String>,
)
