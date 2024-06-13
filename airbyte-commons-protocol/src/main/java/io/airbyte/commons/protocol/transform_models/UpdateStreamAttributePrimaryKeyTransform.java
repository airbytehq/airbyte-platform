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
 * Represents the update of a stream attribute.
 */
@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class UpdateStreamAttributePrimaryKeyTransform {

  private final List<List<String>> oldPrimaryKey;
  private final List<List<String>> newPrimaryKey;

}
