/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels

data class UpdateStreamAttributePrimaryKeyTransform(
  val oldPrimaryKey: List<List<String>>,
  val newPrimaryKey: List<List<String>>,
)
