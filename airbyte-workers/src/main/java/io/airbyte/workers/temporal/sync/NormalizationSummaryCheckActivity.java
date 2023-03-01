/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Optional;

/**
 * Normalization summary check temporal activity interface.
 */
@ActivityInterface
public interface NormalizationSummaryCheckActivity {

  @ActivityMethod
  boolean shouldRunNormalization(Long jobId, Long attemptId, Optional<Long> numCommittedRecords);

}
