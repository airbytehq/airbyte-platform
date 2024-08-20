/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transform_models;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Represents the diff between two fields.
 */
@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public final class StreamAttributeTransform {

  private final StreamAttributeTransformType transformType;
  private final UpdateStreamAttributePrimaryKeyTransform updatePrimaryKeyTransform;
  private final boolean breaking;

  public static StreamAttributeTransform createUpdatePrimaryKeyTransform(final List<List<String>> oldPrimaryKey,
                                                                         final List<List<String>> newPrimaryKey,
                                                                         final Boolean breaking) {
    return new StreamAttributeTransform(StreamAttributeTransformType.UPDATE_PRIMARY_KEY,
        new UpdateStreamAttributePrimaryKeyTransform(oldPrimaryKey, newPrimaryKey),
        breaking);
  }

  public static StreamAttributeTransform createUpdatePrimaryKeyTransform(final UpdateStreamAttributePrimaryKeyTransform updatePrimaryKeyTransform,
                                                                         final Boolean breaking) {
    return new StreamAttributeTransform(StreamAttributeTransformType.UPDATE_PRIMARY_KEY, updatePrimaryKeyTransform,
        breaking);
  }

}
