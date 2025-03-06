/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates jobs db interactions for the metadata domain models.
 */
public class DefaultMetadataPersistence implements MetadataPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetadataPersistence.class);

  private static final String AIRBYTE_METADATA_TABLE = "airbyte_metadata";
  private static final String METADATA_KEY_COL = "key";
  private static final String METADATA_VAL_COL = "value";
  private static final String DEPLOYMENT_ID_KEY = "deployment_id";

  private final ExceptionWrappingDatabase jobDatabase;

  public DefaultMetadataPersistence(final Database database) {
    this.jobDatabase = new ExceptionWrappingDatabase(database);
  }

  @Override
  public Optional<String> getVersion() throws IOException {
    return getMetadata(AirbyteVersion.AIRBYTE_VERSION_KEY_NAME).findFirst();
  }

  @Override
  public void setVersion(final String airbyteVersion) throws IOException {
    // This is not using setMetadata due to the extra (<timestamp>s_init_db, airbyteVersion) that is
    // added to the metadata table
    jobDatabase.query(ctx -> ctx.execute(String.format(
        "INSERT INTO %s(%s, %s) VALUES('%s', '%s'), ('%s_init_db', '%s') ON CONFLICT (%s) DO UPDATE SET %s = '%s'",
        AIRBYTE_METADATA_TABLE,
        METADATA_KEY_COL,
        METADATA_VAL_COL,
        AirbyteVersion.AIRBYTE_VERSION_KEY_NAME,
        airbyteVersion,
        ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
        airbyteVersion,
        METADATA_KEY_COL,
        METADATA_VAL_COL,
        airbyteVersion)));

  }

  @Override
  public Optional<Version> getAirbyteProtocolVersionMax() throws IOException {
    return getMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MAX_KEY_NAME).findFirst().map(Version::new);
  }

  @Override
  public void setAirbyteProtocolVersionMax(final Version version) throws IOException {
    setMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MAX_KEY_NAME, version.serialize());
  }

  @Override
  public Optional<Version> getAirbyteProtocolVersionMin() throws IOException {
    return getMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MIN_KEY_NAME).findFirst().map(Version::new);
  }

  @Override
  public void setAirbyteProtocolVersionMin(final Version version) throws IOException {
    setMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MIN_KEY_NAME, version.serialize());
  }

  @Override
  public Optional<AirbyteProtocolVersionRange> getCurrentProtocolVersionRange() throws IOException {
    final Optional<Version> min = getAirbyteProtocolVersionMin();
    final Optional<Version> max = getAirbyteProtocolVersionMax();

    if (min.isPresent() != max.isPresent()) {
      // Flagging this because this would be highly suspicious but not bad enough that we should fail
      // hard.
      // If the new config is fine, the system should self-heal.
      LOGGER.warn("Inconsistent AirbyteProtocolVersion found, only one of min/max was found. (min:{}, max:{})",
          min.map(Version::serialize).orElse(""), max.map(Version::serialize).orElse(""));
    }

    if (min.isEmpty() && max.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(new AirbyteProtocolVersionRange(min.orElse(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION),
        max.orElse(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION)));
  }

  private Stream<String> getMetadata(final String keyName) throws IOException {
    return jobDatabase.query(ctx -> ctx.select()
        .from(AIRBYTE_METADATA_TABLE)
        .where(DSL.field(METADATA_KEY_COL).eq(keyName))
        .fetch()).stream().map(r -> r.getValue(METADATA_VAL_COL, String.class));
  }

  private void setMetadata(final String keyName, final String value) throws IOException {
    jobDatabase.query(ctx -> ctx
        .insertInto(DSL.table(AIRBYTE_METADATA_TABLE))
        .columns(DSL.field(METADATA_KEY_COL), DSL.field(METADATA_VAL_COL))
        .values(keyName, value)
        .onConflict(DSL.field(METADATA_KEY_COL))
        .doUpdate()
        .set(DSL.field(METADATA_VAL_COL), value)
        .execute());
  }

  @Override
  public Optional<UUID> getDeployment() throws IOException {
    final Result<Record> result = jobDatabase.query(ctx -> ctx.select()
        .from(AIRBYTE_METADATA_TABLE)
        .where(DSL.field(METADATA_KEY_COL).eq(DEPLOYMENT_ID_KEY))
        .fetch());
    return result.stream().findFirst().map(r -> UUID.fromString(r.getValue(METADATA_VAL_COL, String.class)));
  }

  @Override
  public void setDeployment(final UUID deployment) throws IOException {
    // if an existing deployment id already exists, on conflict, return it so we can log it.
    final UUID committedDeploymentId = jobDatabase.query(ctx -> ctx.fetch(String.format(
        "INSERT INTO %s(%s, %s) VALUES('%s', '%s') ON CONFLICT (%s) DO NOTHING RETURNING (SELECT %s FROM %s WHERE %s='%s') as existing_deployment_id",
        AIRBYTE_METADATA_TABLE,
        METADATA_KEY_COL,
        METADATA_VAL_COL,
        DEPLOYMENT_ID_KEY,
        deployment,
        METADATA_KEY_COL,
        METADATA_VAL_COL,
        AIRBYTE_METADATA_TABLE,
        METADATA_KEY_COL,
        DEPLOYMENT_ID_KEY)))
        .stream()
        .filter(record -> record.get("existing_deployment_id", String.class) != null)
        .map(record -> UUID.fromString(record.get("existing_deployment_id", String.class)))
        .findFirst()
        .orElse(deployment); // if no record was returned that means that the new deployment id was used.

    if (!deployment.equals(committedDeploymentId)) {
      LOGGER.warn("Attempted to set a deployment id {}, but deployment id {} already set. Retained original value.", deployment, deployment);
    }
  }

}
