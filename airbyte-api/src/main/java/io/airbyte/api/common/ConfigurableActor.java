/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.common;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Unified interface for actors that are configurable.
 */
public interface ConfigurableActor {

  JsonNode getConfiguration();

}
