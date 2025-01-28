/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import io.airbyte.api.model.generated.FieldAdd;
import io.airbyte.api.model.generated.FieldRemove;
import io.airbyte.api.model.generated.FieldSchemaUpdate;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.StreamAttributePrimaryKeyUpdate;
import io.airbyte.api.model.generated.StreamAttributeTransform;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransformUpdateStream;
import io.airbyte.commons.converters.ApiConverters;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.protocol.transformmodels.FieldTransformType;
import io.airbyte.commons.protocol.transformmodels.StreamAttributeTransformType;
import io.airbyte.commons.protocol.transformmodels.StreamTransformType;
import java.util.Optional;

/**
 * Utility methods for converting between internal and API representation of catalog diffs.
 */
public class CatalogDiffConverters {

  public static StreamTransform streamTransformToApi(final io.airbyte.commons.protocol.transformmodels.StreamTransform transform) {
    return new StreamTransform()
        .transformType(Enums.convertTo(transform.getTransformType(), StreamTransform.TransformTypeEnum.class))
        .streamDescriptor(ApiConverters.toApi(transform.getStreamDescriptor()))
        .updateStream(updateStreamToApi(transform).orElse(null));
  }

  @SuppressWarnings("LineLength")
  public static Optional<StreamTransformUpdateStream> updateStreamToApi(final io.airbyte.commons.protocol.transformmodels.StreamTransform transform) {
    if (transform.getTransformType() == StreamTransformType.UPDATE_STREAM) {
      return Optional.of(new StreamTransformUpdateStream()
          .streamAttributeTransforms(transform.getUpdateStreamTransform()
              .getAttributeTransforms()
              .stream()
              .map(CatalogDiffConverters::streamAttributeTransformToApi)
              .toList())
          .fieldTransforms(transform.getUpdateStreamTransform()
              .getFieldTransforms()
              .stream()
              .map(CatalogDiffConverters::fieldTransformToApi)
              .toList()));
    } else {
      return Optional.empty();
    }
  }

  @SuppressWarnings("LineLength")
  public static StreamAttributeTransform streamAttributeTransformToApi(final io.airbyte.commons.protocol.transformmodels.StreamAttributeTransform transform) {
    return new StreamAttributeTransform()
        .transformType(Enums.convertTo(transform.getTransformType(), StreamAttributeTransform.TransformTypeEnum.class))
        .breaking(transform.getBreaking())
        .updatePrimaryKey(updatePrimaryKeyToApi(transform).orElse(null));
  }

  public static FieldTransform fieldTransformToApi(final io.airbyte.commons.protocol.transformmodels.FieldTransform transform) {
    return new FieldTransform()
        .transformType(Enums.convertTo(transform.getTransformType(), FieldTransform.TransformTypeEnum.class))
        .fieldName(transform.getFieldName())
        .breaking(transform.breaking())
        .addField(addFieldToApi(transform).orElse(null))
        .removeField(removeFieldToApi(transform).orElse(null))
        .updateFieldSchema(updateFieldToApi(transform).orElse(null));
  }

  @SuppressWarnings("LineLength")
  private static Optional<StreamAttributePrimaryKeyUpdate> updatePrimaryKeyToApi(final io.airbyte.commons.protocol.transformmodels.StreamAttributeTransform transform) {
    if (transform.getTransformType() == StreamAttributeTransformType.UPDATE_PRIMARY_KEY) {
      return Optional.of(new StreamAttributePrimaryKeyUpdate()
          .newPrimaryKey(transform.getUpdatePrimaryKeyTransform().getNewPrimaryKey())
          .oldPrimaryKey(transform.getUpdatePrimaryKeyTransform().getOldPrimaryKey()));
    } else {
      return Optional.empty();
    }
  }

  private static Optional<FieldAdd> addFieldToApi(final io.airbyte.commons.protocol.transformmodels.FieldTransform transform) {
    if (transform.getTransformType() == FieldTransformType.ADD_FIELD) {
      return Optional.of(new FieldAdd()
          .schema(transform.getAddFieldTransform().getSchema()));
    } else {
      return Optional.empty();
    }
  }

  private static Optional<FieldRemove> removeFieldToApi(final io.airbyte.commons.protocol.transformmodels.FieldTransform transform) {
    if (transform.getTransformType() == FieldTransformType.REMOVE_FIELD) {
      return Optional.of(new FieldRemove()
          .schema(transform.getRemoveFieldTransform().getSchema()));
    } else {
      return Optional.empty();
    }
  }

  private static Optional<FieldSchemaUpdate> updateFieldToApi(final io.airbyte.commons.protocol.transformmodels.FieldTransform transform) {
    if (transform.getTransformType() == FieldTransformType.UPDATE_FIELD_SCHEMA) {
      return Optional.of(new FieldSchemaUpdate()
          .oldSchema(transform.getUpdateFieldTransform().getOldSchema())
          .newSchema(transform.getUpdateFieldTransform().getNewSchema()));
    } else {
      return Optional.empty();
    }
  }

}
