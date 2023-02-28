/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.models;

import java.util.Optional;

/**
 * Status of a normalization run.
 *
 * @param attemptNumber attempt number
 * @param recordsCommitted num records committed
 * @param normalizationFailed whether normalization failed
 */
public record AttemptNormalizationStatus(int attemptNumber, Optional<Long> recordsCommitted, boolean normalizationFailed) {}
