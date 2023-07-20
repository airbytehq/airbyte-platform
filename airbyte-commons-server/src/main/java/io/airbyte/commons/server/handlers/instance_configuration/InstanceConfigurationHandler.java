/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.instance_configuration;

import io.airbyte.api.model.generated.InstanceConfigurationResponse;

/**
 * Handles requests for the Instance Configuration API endpoint.
 */
public interface InstanceConfigurationHandler {

  InstanceConfigurationResponse getInstanceConfiguration();

}
