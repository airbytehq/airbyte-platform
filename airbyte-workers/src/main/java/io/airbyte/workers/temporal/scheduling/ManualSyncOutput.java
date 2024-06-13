/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ManualSyncOutput.
 */
@Data
@NoArgsConstructor
public class ManualSyncOutput {

  private boolean submitted;

}
