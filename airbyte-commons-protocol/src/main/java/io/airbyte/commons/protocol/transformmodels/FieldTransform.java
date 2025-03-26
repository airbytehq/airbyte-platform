/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Objects;

/**
 * Represents the diff between two fields.
 */
public final class FieldTransform {

  private final FieldTransformType transformType;
  private final List<String> fieldName;
  private final AddFieldTransform addFieldTransform;
  private final RemoveFieldTransform removeFieldTransform;
  private final UpdateFieldSchemaTransform updateFieldTransform;
  private final boolean breaking;

  public FieldTransform(FieldTransformType transformType,
                        List<String> fieldName,
                        AddFieldTransform addFieldTransform,
                        RemoveFieldTransform removeFieldTransform,
                        UpdateFieldSchemaTransform updateFieldTransform,
                        boolean breaking) {
    this.transformType = transformType;
    this.fieldName = fieldName;
    this.addFieldTransform = addFieldTransform;
    this.removeFieldTransform = removeFieldTransform;
    this.updateFieldTransform = updateFieldTransform;
    this.breaking = breaking;
  }

  public static FieldTransform createAddFieldTransform(final List<String> fieldName, final JsonNode schema) {
    return createAddFieldTransform(fieldName, new AddFieldTransform(schema));
  }

  public static FieldTransform createAddFieldTransform(final List<String> fieldName,
                                                       final AddFieldTransform addFieldTransform) {
    return new FieldTransform(FieldTransformType.ADD_FIELD, fieldName, addFieldTransform, null, null, false);
  }

  public static FieldTransform createRemoveFieldTransform(final List<String> fieldName, final JsonNode schema, final Boolean breaking) {
    return createRemoveFieldTransform(fieldName, new RemoveFieldTransform(fieldName, schema), breaking);
  }

  public static FieldTransform createRemoveFieldTransform(final List<String> fieldName,
                                                          final RemoveFieldTransform removeFieldTransform,
                                                          final Boolean breaking) {
    return new FieldTransform(FieldTransformType.REMOVE_FIELD, fieldName, null, removeFieldTransform, null, breaking);
  }

  public static FieldTransform createUpdateFieldTransform(final List<String> fieldName,
                                                          final UpdateFieldSchemaTransform updateFieldTransform) {
    return new FieldTransform(FieldTransformType.UPDATE_FIELD_SCHEMA, fieldName, null, null, updateFieldTransform, false);
  }

  public FieldTransformType getTransformType() {
    return transformType;
  }

  public List<String> getFieldName() {
    return fieldName;
  }

  public AddFieldTransform getAddFieldTransform() {
    return addFieldTransform;
  }

  public RemoveFieldTransform getRemoveFieldTransform() {
    return removeFieldTransform;
  }

  public UpdateFieldSchemaTransform getUpdateFieldTransform() {
    return updateFieldTransform;
  }

  public boolean breaking() {
    return breaking;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldTransform that = (FieldTransform) o;
    return breaking == that.breaking && transformType == that.transformType && Objects.equals(fieldName, that.fieldName)
        && Objects.equals(addFieldTransform, that.addFieldTransform) && Objects.equals(removeFieldTransform,
            that.removeFieldTransform)
        && Objects.equals(updateFieldTransform, that.updateFieldTransform);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transformType, fieldName, addFieldTransform, removeFieldTransform, updateFieldTransform, breaking);
  }

  @Override
  public String toString() {
    return "FieldTransform{"
        + "transformType=" + transformType
        + ", fieldName=" + fieldName
        + ", addFieldTransform=" + addFieldTransform
        + ", removeFieldTransform=" + removeFieldTransform
        + ", updateFieldTransform=" + updateFieldTransform
        + ", breaking=" + breaking
        + '}';
  }

}
