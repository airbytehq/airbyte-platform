/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.airbyte.config.State;

/**
 * Connector Attempt Config Reporting context.
 *
 * @param sourceConfig source configuration used for the attempt
 * @param destinationConfig destination configuration used for the attempt
 * @param state state during the attempt
 */
public record AttemptConfigReportingContext(@Nullable JsonNode sourceConfig, @Nullable JsonNode destinationConfig, @Nullable State state) {}
