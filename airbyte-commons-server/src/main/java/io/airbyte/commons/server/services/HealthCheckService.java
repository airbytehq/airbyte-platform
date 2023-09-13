/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services;

/**
 * Service to check the health of the server.
 */
public interface HealthCheckService {

  boolean healthCheck();

}
