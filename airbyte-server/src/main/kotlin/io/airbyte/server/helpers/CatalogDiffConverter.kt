/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import io.airbyte.commons.enums.convertTo
import io.airbyte.config.CatalogDiff
import io.airbyte.config.FieldSchemaUpdate
import io.airbyte.config.FieldTransform
import io.airbyte.config.StreamAttributePrimaryKeyUpdate
import io.airbyte.config.StreamAttributeTransform
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamTransform
import io.airbyte.config.UpdateStream
import io.airbyte.api.model.generated.CatalogDiff as ApiCatalogDiff
import io.airbyte.api.model.generated.FieldTransform as ApiFieldTransform
import io.airbyte.api.model.generated.StreamAttributeTransform as ApiStreamAttributeTransform
import io.airbyte.api.model.generated.StreamTransform as ApiStreamTransform
import io.airbyte.api.model.generated.StreamTransformUpdateStream as ApiStreamTransformUpdateStream

object CatalogDiffConverter {
  fun ApiCatalogDiff.toDomain(): CatalogDiff {
    val streamTransforms = this.transforms.map { streamTransform -> toDomain(streamTransform) }
    return CatalogDiff().withTransforms(streamTransforms)
  }

  private fun toDomain(streamTransform: ApiStreamTransform): StreamTransform =
    StreamTransform()
      .withTransformType(streamTransform.transformType.convertTo<StreamTransform.TransformType>())
      .withStreamDescriptor(
        StreamDescriptor()
          .withName(streamTransform.streamDescriptor.name)
          .withNamespace(streamTransform.streamDescriptor.namespace),
      ).withUpdateStream(
        toDomain(streamTransform.updateStream),
      )

  private fun toDomain(streamTransformUpdateStream: ApiStreamTransformUpdateStream?): UpdateStream {
    if (streamTransformUpdateStream == null) {
      return UpdateStream()
    }

    return UpdateStream()
      .withFieldTransforms(streamTransformUpdateStream.fieldTransforms.map { fieldTransform -> toDomain(fieldTransform) })
      .withStreamAttributeTransforms(
        streamTransformUpdateStream.streamAttributeTransforms.map { streamAttributeTransform ->
          toDomain(streamAttributeTransform)
        },
      )
  }

  private fun toDomain(fieldTransform: ApiFieldTransform): FieldTransform {
    val result =
      FieldTransform()
        .withTransformType(fieldTransform.transformType.convertTo<FieldTransform.TransformType>())
        .withFieldName(fieldTransform.fieldName)
        .withBreaking(fieldTransform.breaking)
        .withAddField(fieldTransform.addField?.schema)
        .withRemoveField(fieldTransform.removeField?.schema)
        .withUpdateFieldSchema(
          FieldSchemaUpdate()
            .withOldSchema(fieldTransform.updateFieldSchema?.oldSchema)
            .withNewSchema(fieldTransform.updateFieldSchema?.newSchema),
        )

    return result
  }

  private fun toDomain(streamAttributeTransform: ApiStreamAttributeTransform): StreamAttributeTransform =
    StreamAttributeTransform()
      .withTransformType(streamAttributeTransform.transformType.convertTo<StreamAttributeTransform.TransformType>())
      .withBreaking(streamAttributeTransform.breaking)
      .withUpdatePrimaryKey(
        StreamAttributePrimaryKeyUpdate()
          .withOldPrimaryKey(streamAttributeTransform.updatePrimaryKey?.oldPrimaryKey)
          .withNewPrimaryKey(streamAttributeTransform.updatePrimaryKey?.newPrimaryKey),
      )
}
