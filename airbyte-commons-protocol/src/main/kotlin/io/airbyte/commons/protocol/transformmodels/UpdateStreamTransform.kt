/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels

/**
* Represents the update of an [io.airbyte.protocol.models.AirbyteStream].
*/
data class UpdateStreamTransform(
  val fieldTransforms: Set<FieldTransform>,
  val attributeTransforms: Set<StreamAttributeTransform>,
)
