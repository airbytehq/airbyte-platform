/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.DeploymentMetadataRead;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.Result;

@Singleton
public class DeploymentMetadataHandler {

  private static final String DEPLOYMENT_ID_QUERY = "SELECT value FROM airbyte_metadata where key = 'deployment_id'";

  private final AirbyteVersion airbyteVersion;
  private final Configs.DeploymentMode deploymentMode;
  private final DSLContext dslContext;

  public DeploymentMetadataHandler(
                                   final AirbyteVersion airbyteVersion,
                                   final Configs.DeploymentMode deploymentMode,
                                   @Named("unwrappedConfig") final DSLContext dslContext) {
    this.airbyteVersion = airbyteVersion;
    this.deploymentMode = deploymentMode;
    this.dslContext = dslContext;
  }

  public DeploymentMetadataRead getDeploymentMetadata() {
    final Result<org.jooq.Record> result = dslContext.fetch(DEPLOYMENT_ID_QUERY);
    final String deploymentId = result.getValue(0, "value").toString();
    return new DeploymentMetadataRead().id(UUID.fromString(deploymentId))
        .mode(deploymentMode.name())
        .version(airbyteVersion.serialize());
  }

}
