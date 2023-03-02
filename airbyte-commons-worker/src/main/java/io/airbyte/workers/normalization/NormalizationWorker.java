/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.normalization;

import io.airbyte.config.NormalizationInput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.workers.Worker;

/**
 * Worker that runs normalization.
 */
public interface NormalizationWorker extends Worker<NormalizationInput, NormalizationSummary> {}
