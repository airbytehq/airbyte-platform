/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ConnectionUpdaterWorkflowConfig.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionUpdaterWorkflowConfig {

  private boolean firstStart;

}
