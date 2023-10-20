/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG_FETCH_EVENT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogFetchEvent;
import io.airbyte.config.ActorCatalogWithUpdatedAt;
import io.airbyte.config.ConfigSchema;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.CatalogService;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.protocol.models.AirbyteCatalog;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class CatalogServiceJooqImpl implements CatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceJooqImpl.class);
  private final ExceptionWrappingDatabase database;

  @VisibleForTesting
  public CatalogServiceJooqImpl(@Named("configDatabase") final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Get actor catalog.
   *
   * @param actorCatalogId actor catalog id
   * @return actor catalog
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public ActorCatalog getActorCatalogById(UUID actorCatalogId)
      throws IOException, ConfigNotFoundException {
    final Result<Record> result = database.query(ctx -> ctx.select(ACTOR_CATALOG.asterisk())
        .from(ACTOR_CATALOG).where(ACTOR_CATALOG.ID.eq(actorCatalogId))).fetch();

    if (result.size() > 0) {
      return DbConverter.buildActorCatalog(result.get(0));
    }
    throw new ConfigNotFoundException(ConfigSchema.ACTOR_CATALOG, actorCatalogId);
  }

  /**
   * Get most actor catalog for source.
   *
   * @param actorId actor id
   * @param actorVersion actor definition version used to make this actor
   * @param configHash config hash for actor
   * @return actor catalog for config has and actor version
   * @throws IOException - error while interacting with db
   */
  @Override
  public Optional<ActorCatalog> getActorCatalog(UUID actorId,
                                                String actorVersion,
                                                String configHash)
      throws IOException {
    final Result<Record> records = database.transaction(ctx -> ctx.select(ACTOR_CATALOG.asterisk())
        .from(ACTOR_CATALOG).join(ACTOR_CATALOG_FETCH_EVENT)
        .on(ACTOR_CATALOG.ID.eq(ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID))
        .where(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(actorId))
        .and(ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION.eq(actorVersion))
        .and(ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH.eq(configHash))
        .orderBy(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc()).limit(1)).fetch();

    return records.stream().findFirst().map(DbConverter::buildActorCatalog);
  }

  /**
   * Get most recent actor catalog for source.
   *
   * @param sourceId source id
   * @return current actor catalog with updated at
   * @throws IOException - error while interacting with db
   */
  @Override
  public Optional<ActorCatalogWithUpdatedAt> getMostRecentSourceActorCatalog(UUID sourceId)
      throws IOException {
    final Result<Record> records = database.query(ctx -> ctx.select(ACTOR_CATALOG.asterisk(), ACTOR_CATALOG_FETCH_EVENT.CREATED_AT)
        .from(ACTOR_CATALOG)
        .join(ACTOR_CATALOG_FETCH_EVENT)
        .on(ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID.eq(ACTOR_CATALOG.ID))
        .where(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
        .orderBy(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc()).limit(1).fetch());
    return records.stream().findFirst().map(DbConverter::buildActorCatalogWithUpdatedAt);
  }

  /**
   * Get most recent actor catalog for source.
   *
   * @param sourceId source id
   * @return current actor catalog
   * @throws IOException - error while interacting with db
   */
  @Override
  public Optional<ActorCatalog> getMostRecentActorCatalogForSource(UUID sourceId)
      throws IOException {
    final Result<Record> records = database.query(ctx -> ctx.select(ACTOR_CATALOG.asterisk())
        .from(ACTOR_CATALOG)
        .join(ACTOR_CATALOG_FETCH_EVENT)
        .on(ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID.eq(ACTOR_CATALOG.ID))
        .where(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
        .orderBy(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc()).limit(1).fetch());
    return records.stream().findFirst().map(DbConverter::buildActorCatalog);
  }

  /**
   * Get most recent actor catalog fetch event for source.
   *
   * @param sourceId source id
   * @return last actor catalog fetch event
   * @throws IOException - error while interacting with db
   */
  @Override
  public Optional<ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSource(
                                                                                       UUID sourceId)
      throws IOException {
    final Result<Record> records = database.query(ctx -> ctx.select(ACTOR_CATALOG_FETCH_EVENT.asterisk())
        .from(ACTOR_CATALOG_FETCH_EVENT)
        .where(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
        .orderBy(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc()).limit(1).fetch());
    return records.stream().findFirst().map(DbConverter::buildActorCatalogFetchEvent);
  }

  /**
   * Stores source catalog information.
   * <p>
   * This function is called each time the schema of a source is fetched. This can occur because the
   * source is set up for the first time, because the configuration or version of the connector
   * changed or because the user explicitly requested a schema refresh. Schemas are stored separately
   * and de-duplicated upon insertion. Once a schema has been successfully stored, a call to
   * getActorCatalog(actorId, connectionVersion, configurationHash) will return the most recent schema
   * stored for those parameters.
   *
   * @param catalog - catalog that was fetched.
   * @param actorId - actor the catalog was fetched by
   * @param connectorVersion - version of the connector when catalog was fetched
   * @param configurationHash - hash of the config of the connector when catalog was fetched
   * @return The identifier (UUID) of the fetch event inserted in the database
   * @throws IOException - error while interacting with db
   */
  @Override
  public UUID writeActorCatalogFetchEvent(AirbyteCatalog catalog,
                                          UUID actorId,
                                          String connectorVersion,
                                          String configurationHash)
      throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final UUID fetchEventID = UUID.randomUUID();
    return database.transaction(ctx -> {
      final UUID catalogId = getOrInsertActorCatalog(catalog, ctx, timestamp);
      ctx.insertInto(ACTOR_CATALOG_FETCH_EVENT)
          .set(ACTOR_CATALOG_FETCH_EVENT.ID, fetchEventID)
          .set(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID, actorId)
          .set(ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID, catalogId)
          .set(ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH, configurationHash)
          .set(ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION, connectorVersion)
          .set(ACTOR_CATALOG_FETCH_EVENT.MODIFIED_AT, timestamp)
          .set(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT, timestamp).execute();
      return catalogId;
    });
  }

  /**
   * Get most recent actor catalog fetch event for sources.
   *
   * @param sourceIds source ids
   * @return map of source id to the last actor catalog fetch event
   * @throws IOException - error while interacting with db
   */
  @Override
  public Map<UUID, ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSources(
                                                                                         List<UUID> sourceIds)
      throws IOException {
    // noinspection SqlResolve
    if (sourceIds.isEmpty()) {
      return Collections.emptyMap();
    }
    return database.query(ctx -> ctx.fetch(
        """
        select distinct actor_catalog_id, actor_id, created_at from
          (select
            actor_catalog_id,
            actor_id,
            created_at,
            row_number() over (partition by actor_id order by created_at desc) as creation_order_row_number
          from public.actor_catalog_fetch_event
          where actor_id in ({0})
          ) table_with_rank
        where creation_order_row_number = 1;
        """,
        DSL.list(sourceIds.stream().map(DSL::value).collect(Collectors.toList()))))
        .stream().map(DbConverter::buildActorCatalogFetchEvent)
        .collect(Collectors.toMap(ActorCatalogFetchEvent::getActorId, record -> record));
  }

  /**
   * Store an Airbyte catalog in DB if it is not present already. Checks in the config DB if the
   * catalog is present already, if so returns it identifier. If not present, it is inserted in DB
   * with a new identifier and that identifier is returned.
   *
   * @param airbyteCatalog the catalog to be cached
   * @param context - db context
   * @param timestamp - timestamp
   * @return the db identifier for the cached catalog.
   */
  private UUID getOrInsertActorCatalog(final AirbyteCatalog airbyteCatalog,
                                       final DSLContext context,
                                       final OffsetDateTime timestamp) {

    final String canonicalCatalogHash = generateCanonicalHash(airbyteCatalog);
    UUID catalogId = lookupCatalogId(canonicalCatalogHash, airbyteCatalog, context);
    if (catalogId != null) {
      return catalogId;
    }

    final String oldCatalogHash = generateOldHash(airbyteCatalog);
    catalogId = lookupCatalogId(oldCatalogHash, airbyteCatalog, context);
    if (catalogId != null) {
      return catalogId;
    }

    return insertCatalog(airbyteCatalog, canonicalCatalogHash, context, timestamp);
  }

  private String generateCanonicalHash(final AirbyteCatalog airbyteCatalog) {
    final HashFunction hashFunction = Hashing.murmur3_32_fixed();
    try {
      return hashFunction.hashBytes(Jsons.canonicalJsonSerialize(airbyteCatalog)
          .getBytes(Charsets.UTF_8)).toString();
    } catch (final IOException e) {
      LOGGER.error("Failed to serialize AirbyteCatalog to canonical JSON", e);
      return null;
    }
  }

  private UUID lookupCatalogId(final String catalogHash, final AirbyteCatalog airbyteCatalog, final DSLContext context) {
    if (catalogHash == null) {
      return null;
    }
    return findAndReturnCatalogId(catalogHash, airbyteCatalog, context);
  }

  private String generateOldHash(final AirbyteCatalog airbyteCatalog) {
    final HashFunction hashFunction = Hashing.murmur3_32_fixed();
    return hashFunction.hashBytes(Jsons.serialize(airbyteCatalog).getBytes(Charsets.UTF_8)).toString();
  }

  private UUID insertCatalog(final AirbyteCatalog airbyteCatalog,
                             final String catalogHash,
                             final DSLContext context,
                             final OffsetDateTime timestamp) {
    final UUID catalogId = UUID.randomUUID();
    context.insertInto(ACTOR_CATALOG)
        .set(ACTOR_CATALOG.ID, catalogId)
        .set(ACTOR_CATALOG.CATALOG, JSONB.valueOf(Jsons.serialize(airbyteCatalog)))
        .set(ACTOR_CATALOG.CATALOG_HASH, catalogHash)
        .set(ACTOR_CATALOG.CREATED_AT, timestamp)
        .set(ACTOR_CATALOG.MODIFIED_AT, timestamp).execute();
    return catalogId;
  }

  private UUID findAndReturnCatalogId(final String catalogHash, final AirbyteCatalog airbyteCatalog, final DSLContext context) {
    final Map<UUID, AirbyteCatalog> catalogs = findCatalogByHash(catalogHash, context);
    for (final Map.Entry<UUID, AirbyteCatalog> entry : catalogs.entrySet()) {
      if (entry.getValue().equals(airbyteCatalog)) {
        return entry.getKey();
      }
    }
    return null;
  }

  private Map<UUID, AirbyteCatalog> findCatalogByHash(final String catalogHash, final DSLContext context) {
    final Result<Record2<UUID, JSONB>> records = context.select(ACTOR_CATALOG.ID, ACTOR_CATALOG.CATALOG)
        .from(ACTOR_CATALOG)
        .where(ACTOR_CATALOG.CATALOG_HASH.eq(catalogHash)).fetch();

    final Map<UUID, AirbyteCatalog> result = new HashMap<>();
    for (final Record record : records) {
      // We do not apply the on-the-fly migration here because the only caller is getOrInsertActorCatalog
      // which is using this to figure out if the catalog has already been inserted. Migrating on the fly
      // here will cause us to add a duplicate each time we check for existence of a catalog.
      final AirbyteCatalog catalog = Jsons.deserialize(record.get(ACTOR_CATALOG.CATALOG).toString(), AirbyteCatalog.class);
      result.put(record.get(ACTOR_CATALOG.ID), catalog);
    }
    return result;
  }

}
