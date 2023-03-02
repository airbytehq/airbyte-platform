/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.workers.Worker;

/**
 * Discover Catalog Worker. Calls discover method on a connector.
 */
public interface DiscoverCatalogWorker extends Worker<StandardDiscoverCatalogInput, ConnectorJobOutput> {}
