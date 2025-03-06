/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.common;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Unified interface for actors that are configurable.
 */
public interface ConfigurableActor {

  JsonNode getConfiguration();

}
