/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.commons.enums.convertTo
import io.airbyte.api.client.model.generated.CatalogDiff as ApiCatalogDiff
import io.airbyte.api.client.model.generated.FieldTransform as ApiFieldTransform
import io.airbyte.api.client.model.generated.StreamAttributeTransform as ApiStreamAttributeTransform
import io.airbyte.api.client.model.generated.StreamTransform as ApiStreamTransform
import io.airbyte.api.client.model.generated.StreamTransformUpdateStream as ApiStreamTransformUpdateStream
import io.airbyte.config.CatalogDiff as DomainCatalogDiff
import io.airbyte.config.FieldSchemaUpdate as DomainFieldSchemaUpdate
import io.airbyte.config.FieldTransform as DomainFieldTransform
import io.airbyte.config.StreamAttributePrimaryKeyUpdate as DomainStreamAttributePrimaryKeyUpdate
import io.airbyte.config.StreamAttributeTransform as DomainStreamAttributeTransform
import io.airbyte.config.StreamDescriptor as DomainStreamDescriptor
import io.airbyte.config.StreamTransform as DomainStreamTransform
import io.airbyte.config.UpdateStream as DomainUpdateStream

object CatalogDiffConverter {
  @JvmStatic
  fun toDomain(domainCatalogDiff: ApiCatalogDiff): DomainCatalogDiff {
    val streamTransforms =
      domainCatalogDiff.transforms
        .map { streamTransform -> toDomain(streamTransform) }

    return DomainCatalogDiff()
      .withTransforms(streamTransforms)
  }

  private fun toDomain(streamTransform: ApiStreamTransform): DomainStreamTransform =
    DomainStreamTransform()
      .withTransformType(streamTransform.transformType.convertTo<DomainStreamTransform.TransformType>())
      .withStreamDescriptor(
        DomainStreamDescriptor()
          .withName(streamTransform.streamDescriptor.name)
          .withNamespace(streamTransform.streamDescriptor.namespace),
      ).withUpdateStream(
        toDomain(streamTransform.updateStream),
      )

  private fun toDomain(streamTransformUpdateStream: ApiStreamTransformUpdateStream?): DomainUpdateStream {
    if (streamTransformUpdateStream == null) {
      return DomainUpdateStream()
    }

    return DomainUpdateStream()
      .withFieldTransforms(streamTransformUpdateStream.fieldTransforms.map { fieldTransform -> toDomain(fieldTransform) })
      .withStreamAttributeTransforms(
        streamTransformUpdateStream.streamAttributeTransforms.map { streamAttributeTransform ->
          toDomain(streamAttributeTransform)
        },
      )
  }

  private fun toDomain(fieldTransform: ApiFieldTransform): DomainFieldTransform {
    val result =
      DomainFieldTransform()
        .withTransformType(fieldTransform.transformType.convertTo<DomainFieldTransform.TransformType>())
        .withFieldName(fieldTransform.fieldName)
        .withBreaking(fieldTransform.breaking)
        .withAddField(fieldTransform.addField?.schema)
        .withRemoveField(fieldTransform.removeField?.schema)
        .withUpdateFieldSchema(
          DomainFieldSchemaUpdate()
            .withOldSchema(fieldTransform.updateFieldSchema?.oldSchema)
            .withNewSchema(fieldTransform.updateFieldSchema?.newSchema),
        )

    // if (fieldTransform.addField != null) {
    //   result.addField = fieldTransform.addField?.schema
    // }
    //
    // if (fieldTransform.removeField != null) {
    //
    // }

    return result
  }

  private fun toDomain(streamAttributeTransform: ApiStreamAttributeTransform): DomainStreamAttributeTransform =
    DomainStreamAttributeTransform()
      .withTransformType(streamAttributeTransform.transformType.convertTo<DomainStreamAttributeTransform.TransformType>())
      .withBreaking(streamAttributeTransform.breaking)
      .withUpdatePrimaryKey(
        DomainStreamAttributePrimaryKeyUpdate()
          .withOldPrimaryKey(streamAttributeTransform.updatePrimaryKey?.oldPrimaryKey)
          .withNewPrimaryKey(streamAttributeTransform.updatePrimaryKey?.newPrimaryKey),
      )
}
