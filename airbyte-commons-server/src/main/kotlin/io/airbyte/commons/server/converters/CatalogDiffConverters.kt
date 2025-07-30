/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.FieldAdd
import io.airbyte.api.model.generated.FieldRemove
import io.airbyte.api.model.generated.FieldSchemaUpdate
import io.airbyte.api.model.generated.StreamAttributePrimaryKeyUpdate
import io.airbyte.api.model.generated.StreamTransformUpdateStream
import io.airbyte.commons.converters.ApiConverters.Companion.toApi
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.protocol.transformmodels.FieldTransform
import io.airbyte.commons.protocol.transformmodels.FieldTransformType
import io.airbyte.commons.protocol.transformmodels.StreamAttributeTransform
import io.airbyte.commons.protocol.transformmodels.StreamAttributeTransformType
import io.airbyte.commons.protocol.transformmodels.StreamTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransformType
import java.util.Optional

/**
 * Utility methods for converting between internal and API representation of catalog diffs.
 */
object CatalogDiffConverters {
  @JvmStatic
  fun streamTransformToApi(transform: StreamTransform): io.airbyte.api.model.generated.StreamTransform =
    io.airbyte.api.model.generated
      .StreamTransform()
      .transformType(
        transform.transformType?.convertTo<io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum>(),
      ).streamDescriptor(transform.streamDescriptor.toApi())
      .updateStream(updateStreamToApi(transform).orElse(null))

  fun updateStreamToApi(transform: StreamTransform): Optional<StreamTransformUpdateStream> =
    if (transform.transformType == StreamTransformType.UPDATE_STREAM) {
      Optional.of(
        StreamTransformUpdateStream()
          .streamAttributeTransforms(
            transform.updateStreamTransform
              ?.attributeTransforms
              ?.stream()
              ?.map { obj: StreamAttributeTransform -> streamAttributeTransformToApi(obj) }
              ?.toList() ?: emptyList(),
          ).fieldTransforms(
            transform.updateStreamTransform
              ?.fieldTransforms
              ?.stream()
              ?.map { obj: FieldTransform -> fieldTransformToApi(obj) }
              ?.toList() ?: emptyList(),
          ),
      )
    } else {
      Optional.empty()
    }

  fun streamAttributeTransformToApi(transform: StreamAttributeTransform): io.airbyte.api.model.generated.StreamAttributeTransform =
    io.airbyte.api.model.generated
      .StreamAttributeTransform()
      .transformType(
        transform.transformType?.convertTo<io.airbyte.api.model.generated.StreamAttributeTransform.TransformTypeEnum>(),
      ).breaking(transform.breaking)
      .updatePrimaryKey(updatePrimaryKeyToApi(transform).orElse(null))

  fun fieldTransformToApi(transform: FieldTransform): io.airbyte.api.model.generated.FieldTransform =
    io.airbyte.api.model.generated
      .FieldTransform()
      .transformType(
        transform.transformType?.convertTo<io.airbyte.api.model.generated.FieldTransform.TransformTypeEnum>(),
      ).fieldName(transform.fieldName)
      .breaking(transform.breaking())
      .addField(addFieldToApi(transform).orElse(null))
      .removeField(removeFieldToApi(transform).orElse(null))
      .updateFieldSchema(updateFieldToApi(transform).orElse(null))

  private fun updatePrimaryKeyToApi(transform: StreamAttributeTransform): Optional<StreamAttributePrimaryKeyUpdate> =
    if (transform.transformType == StreamAttributeTransformType.UPDATE_PRIMARY_KEY) {
      Optional.of(
        StreamAttributePrimaryKeyUpdate()
          .newPrimaryKey(transform.updatePrimaryKeyTransform!!.newPrimaryKey)
          .oldPrimaryKey(transform.updatePrimaryKeyTransform!!.oldPrimaryKey),
      )
    } else {
      Optional.empty()
    }

  private fun addFieldToApi(transform: FieldTransform): Optional<FieldAdd> =
    if (transform.transformType == FieldTransformType.ADD_FIELD) {
      Optional.of(
        FieldAdd()
          .schema(transform.addFieldTransform!!.schema),
      )
    } else {
      Optional.empty()
    }

  private fun removeFieldToApi(transform: FieldTransform): Optional<FieldRemove> =
    if (transform.transformType == FieldTransformType.REMOVE_FIELD) {
      Optional.of(
        FieldRemove()
          .schema(transform.removeFieldTransform!!.schema),
      )
    } else {
      Optional.empty()
    }

  private fun updateFieldToApi(transform: FieldTransform): Optional<FieldSchemaUpdate> =
    if (transform.transformType == FieldTransformType.UPDATE_FIELD_SCHEMA) {
      Optional.of(
        FieldSchemaUpdate()
          .oldSchema(transform.updateFieldTransform!!.oldSchema)
          .newSchema(transform.updateFieldTransform!!.newSchema),
      )
    } else {
      Optional.empty()
    }
}
