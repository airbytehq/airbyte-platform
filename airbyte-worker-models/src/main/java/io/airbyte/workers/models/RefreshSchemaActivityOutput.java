/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.config.CatalogDiff;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A class holding the output to the Temporal schema refresh activity.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RefreshSchemaActivityOutput {

  private CatalogDiff appliedDiff;

}
