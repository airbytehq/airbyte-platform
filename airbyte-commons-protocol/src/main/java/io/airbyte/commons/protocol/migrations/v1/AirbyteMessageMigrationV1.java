/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations.v1;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.protocol.migrations.AirbyteMessageMigration;
import io.airbyte.commons.version.Version;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.validation.json.JsonSchemaValidator;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;

/**
 * V1 Migration.
 */
// Disable V1 Migration, uncomment to re-enable
// @Singleton
public class AirbyteMessageMigrationV1 implements AirbyteMessageMigration<io.airbyte.protocol.models.v0.AirbyteMessage, AirbyteMessage> {

  private final JsonSchemaValidator validator;

  public AirbyteMessageMigrationV1() {
    this(new JsonSchemaValidator());
  }

  @VisibleForTesting
  public AirbyteMessageMigrationV1(final JsonSchemaValidator validator) {
    this.validator = validator;
  }

  @Override
  public io.airbyte.protocol.models.v0.AirbyteMessage downgrade(AirbyteMessage message, Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    throw new NotImplementedException("Migration not implemented.");
  }

  @Override
  public AirbyteMessage upgrade(io.airbyte.protocol.models.v0.AirbyteMessage message, Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    throw new NotImplementedException("Migration not implemented.");
  }

  @Override
  public Version getPreviousVersion() {
    return null;
  }

  @Override
  public Version getCurrentVersion() {
    return null;
  }

}
