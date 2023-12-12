/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.api.client.model.generated.CatalogDiff;
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
