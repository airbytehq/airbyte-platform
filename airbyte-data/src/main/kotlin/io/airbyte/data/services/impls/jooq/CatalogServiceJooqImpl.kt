/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import datadog.trace.api.Trace
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorCatalog.CatalogType
import io.airbyte.config.ActorCatalogFetchEvent
import io.airbyte.config.ActorCatalogWithUpdatedAt
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.CatalogService
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorCatalogType
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.DestinationCatalog
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors

@Singleton
class CatalogServiceJooqImpl
  @VisibleForTesting
  constructor(
    @Named("configDatabase") database: Database?,
  ) : CatalogService {
    private val database = ExceptionWrappingDatabase(database)

    /**
     * Get actor catalog.
     *
     * @param actorCatalogId actor catalog id
     * @return actor catalog
     * @throws ConfigNotFoundException if the config does not exist
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    override fun getActorCatalogById(actorCatalogId: UUID): ActorCatalog {
      val result =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(Tables.ACTOR_CATALOG.asterisk())
              .from(Tables.ACTOR_CATALOG)
              .where(Tables.ACTOR_CATALOG.ID.eq(actorCatalogId))
          }.fetch()

      if (!result.isEmpty()) {
        return DbConverter.buildActorCatalog(result[0])
      }
      throw ConfigNotFoundException(ConfigNotFoundType.ACTOR_CATALOG, actorCatalogId)
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
    @Throws(IOException::class)
    override fun getActorCatalog(
      actorId: UUID,
      actorVersion: String,
      configHash: String,
    ): Optional<ActorCatalog> {
      val records =
        database
          .transaction { ctx: DSLContext ->
            ctx
              .select(Tables.ACTOR_CATALOG.asterisk())
              .from(Tables.ACTOR_CATALOG)
              .join(Tables.ACTOR_CATALOG_FETCH_EVENT)
              .on(Tables.ACTOR_CATALOG.ID.eq(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID))
              .where(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(actorId))
              .and(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION.eq(actorVersion))
              .and(Tables.ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH.eq(configHash))
              .orderBy(Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc())
              .limit(1)
          }.fetch()

      return records.stream().findFirst().map { record: Record -> DbConverter.buildActorCatalog(record) }
    }

    /**
     * Get most recent actor catalog for source.
     *
     * @param sourceId source id
     * @return current actor catalog with updated at
     * @throws IOException - error while interacting with db
     */
    @Throws(IOException::class)
    override fun getMostRecentSourceActorCatalog(sourceId: UUID): Optional<ActorCatalogWithUpdatedAt> {
      val records =
        database.query { ctx: DSLContext ->
          ctx
            .select(
              Tables.ACTOR_CATALOG.asterisk(),
              Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT,
            ).from(Tables.ACTOR_CATALOG)
            .join(Tables.ACTOR_CATALOG_FETCH_EVENT)
            .on(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID.eq(Tables.ACTOR_CATALOG.ID))
            .where(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
            .orderBy(Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc())
            .limit(1)
            .fetch()
        }
      return records.stream().findFirst().map { record: Record ->
        DbConverter.buildActorCatalogWithUpdatedAt(
          record,
        )
      }
    }

    /**
     * Get most recent actor catalog for source.
     *
     * @param sourceId source id
     * @return current actor catalog
     * @throws IOException - error while interacting with db
     */
    @Throws(IOException::class)
    override fun getMostRecentActorCatalogForSource(sourceId: UUID): Optional<ActorCatalog> {
      val records =
        database.query { ctx: DSLContext ->
          ctx
            .select(Tables.ACTOR_CATALOG.asterisk())
            .from(Tables.ACTOR_CATALOG)
            .join(Tables.ACTOR_CATALOG_FETCH_EVENT)
            .on(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID.eq(Tables.ACTOR_CATALOG.ID))
            .where(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
            .orderBy(Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc())
            .limit(1)
            .fetch()
        }
      return records.stream().findFirst().map { record: Record -> DbConverter.buildActorCatalog(record) }
    }

    /**
     * Get most recent actor catalog fetch event for source.
     *
     * @param sourceId source id
     * @return last actor catalog fetch event
     * @throws IOException - error while interacting with db
     */
    @Throws(IOException::class)
    override fun getMostRecentActorCatalogFetchEventForSource(sourceId: UUID): Optional<ActorCatalogFetchEvent> {
      val records =
        database.query { ctx: DSLContext ->
          ctx
            .select(Tables.ACTOR_CATALOG_FETCH_EVENT.asterisk())
            .from(Tables.ACTOR_CATALOG_FETCH_EVENT)
            .where(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
            .orderBy(Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc())
            .limit(1)
            .fetch()
        }
      return records.stream().findFirst().map { record: Record ->
        DbConverter.buildActorCatalogFetchEvent(
          record,
        )
      }
    }

    /**
     * Stores source catalog information.
     *
     *
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
    @Throws(IOException::class)
    override fun writeActorCatalogWithFetchEvent(
      catalog: AirbyteCatalog,
      actorId: UUID,
      connectorVersion: String,
      configurationHash: String,
    ): UUID {
      val timestamp = OffsetDateTime.now()
      val fetchEventID = UUID.randomUUID()
      return database.transaction<UUID> { ctx: DSLContext ->
        val catalogId = getOrInsertSourceActorCatalog(catalog, ctx, timestamp)
        ctx
          .insertInto(Tables.ACTOR_CATALOG_FETCH_EVENT)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.ID, fetchEventID)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID, actorId)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID, catalogId)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH, configurationHash)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION, connectorVersion)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.MODIFIED_AT, timestamp)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT, timestamp)
          .execute()
        catalogId
      }
    }

    @Throws(IOException::class)
    override fun writeActorCatalogWithFetchEvent(
      catalog: DestinationCatalog,
      actorId: UUID,
      connectorVersion: String,
      configurationHash: String,
    ): UUID {
      val timestamp = OffsetDateTime.now()
      val fetchEventID = UUID.randomUUID()
      return database.transaction<UUID> { ctx: DSLContext ->
        val catalogId = getOrInsertDestinationActorCatalog(catalog, ctx, timestamp)
        ctx
          .insertInto(Tables.ACTOR_CATALOG_FETCH_EVENT)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.ID, fetchEventID)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID, actorId)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID, catalogId)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH, configurationHash)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION, connectorVersion)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.MODIFIED_AT, timestamp)
          .set(Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT, timestamp)
          .execute()
        catalogId
      }
    }

    /**
     * Get most recent actor catalog fetch event for sources.
     *
     * @param sourceIds source ids
     * @return map of source id to the last actor catalog fetch event
     * @throws IOException - error while interacting with db
     */
    @Trace
    @Throws(IOException::class)
    override fun getMostRecentActorCatalogFetchEventForSources(sourceIds: List<UUID>): Map<UUID, ActorCatalogFetchEvent> {
      // noinspection SqlResolve
      if (sourceIds.isEmpty()) {
        return emptyMap()
      }
      return database
        .query { ctx: DSLContext ->
          ctx.fetch(
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
            
            """.trimIndent(),
            DSL.list(
              sourceIds
                .stream()
                .map { value: UUID? -> DSL.value(value) }
                .collect(Collectors.toList()),
            ),
          )
        }.stream()
        .map { record: Record -> DbConverter.buildActorCatalogFetchEvent(record) }
        .collect(
          Collectors.toMap(
            { obj: ActorCatalogFetchEvent -> obj.actorId },
            { record: ActorCatalogFetchEvent -> record },
          ),
        )
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
    private fun getOrInsertSourceActorCatalog(
      airbyteCatalog: AirbyteCatalog,
      context: DSLContext,
      timestamp: OffsetDateTime,
    ): UUID {
      val canonicalCatalogHash = generateCanonicalHash(airbyteCatalog)
      val catalogJson = Jsons.jsonNode(airbyteCatalog)
      var catalogId = lookupCatalogId(canonicalCatalogHash, catalogJson, context)
      if (catalogId != null) {
        return catalogId
      }

      val oldCatalogHash = generateOldHash(airbyteCatalog)
      catalogId = lookupCatalogId(oldCatalogHash, catalogJson, context)
      if (catalogId != null) {
        return catalogId
      }

      return insertCatalog(catalogJson, canonicalCatalogHash, CatalogType.SOURCE_CATALOG, context, timestamp)
    }

    private fun getOrInsertDestinationActorCatalog(
      destinationCatalog: DestinationCatalog,
      context: DSLContext,
      timestamp: OffsetDateTime,
    ): UUID {
      val canonicalCatalogHash = generateCanonicalHash(destinationCatalog)
      val catalogJson = Jsons.jsonNode(destinationCatalog)
      val catalogId = lookupCatalogId(canonicalCatalogHash, catalogJson, context)
      if (catalogId != null) {
        return catalogId
      }

      return insertCatalog(catalogJson, canonicalCatalogHash, CatalogType.DESTINATION_CATALOG, context, timestamp)
    }

    private fun generateCanonicalHash(airbyteCatalog: AirbyteCatalog): String? {
      val hashFunction = Hashing.murmur3_32_fixed()
      try {
        return hashFunction
          .hashBytes(
            Jsons
              .canonicalJsonSerialize(airbyteCatalog)
              .toByteArray(Charsets.UTF_8),
          ).toString()
      } catch (e: IOException) {
        log.error(e) { "Failed to serialize AirbyteCatalog to canonical JSON" }
        return null
      }
    }

    private fun generateCanonicalHash(destinationCatalog: DestinationCatalog): String? {
      val hashFunction = Hashing.murmur3_32_fixed()
      try {
        return hashFunction
          .hashBytes(
            Jsons
              .canonicalJsonSerialize(destinationCatalog)
              .toByteArray(Charsets.UTF_8),
          ).toString()
      } catch (e: IOException) {
        log.error(e) { "Failed to serialize DestinationCatalog to canonical JSON" }
        return null
      }
    }

    private fun lookupCatalogId(
      catalogHash: String?,
      catalog: JsonNode,
      context: DSLContext,
    ): UUID? {
      if (catalogHash == null) {
        return null
      }
      return findAndReturnCatalogId(catalogHash, catalog, context)
    }

    private fun generateOldHash(airbyteCatalog: AirbyteCatalog): String {
      val hashFunction = Hashing.murmur3_32_fixed()
      return hashFunction.hashBytes(Jsons.serialize(airbyteCatalog).toByteArray(Charsets.UTF_8)).toString()
    }

    private fun insertCatalog(
      catalog: JsonNode,
      catalogHash: String?,
      catalogType: CatalogType,
      context: DSLContext,
      timestamp: OffsetDateTime,
    ): UUID {
      val catalogId = UUID.randomUUID()
      context
        .insertInto(Tables.ACTOR_CATALOG)
        .set(Tables.ACTOR_CATALOG.ID, catalogId)
        .set(Tables.ACTOR_CATALOG.CATALOG, JSONB.valueOf(Jsons.serialize(catalog)))
        .set(Tables.ACTOR_CATALOG.CATALOG_HASH, catalogHash)
        .set(Tables.ACTOR_CATALOG.CATALOG_TYPE, ActorCatalogType.valueOf(catalogType.toString()))
        .set(Tables.ACTOR_CATALOG.CREATED_AT, timestamp)
        .set(Tables.ACTOR_CATALOG.MODIFIED_AT, timestamp)
        .execute()
      return catalogId
    }

    private fun findAndReturnCatalogId(
      catalogHash: String,
      catalog: JsonNode,
      context: DSLContext,
    ): UUID? {
      val catalogs = findCatalogByHash(catalogHash, context)
      for ((key, value) in catalogs) {
        if (value == catalog) {
          return key
        }
      }
      return null
    }

    private fun findCatalogByHash(
      catalogHash: String,
      context: DSLContext,
    ): Map<UUID, JsonNode> {
      val records =
        context
          .select(Tables.ACTOR_CATALOG.ID, Tables.ACTOR_CATALOG.CATALOG)
          .from(Tables.ACTOR_CATALOG)
          .where(Tables.ACTOR_CATALOG.CATALOG_HASH.eq(catalogHash))
          .fetch()

      val result: MutableMap<UUID, JsonNode> = HashMap()
      for (record in records) {
        result[record.get(Tables.ACTOR_CATALOG.ID)] =
          Jsons.deserialize(
            record.get(Tables.ACTOR_CATALOG.CATALOG).toString(),
          )
      }
      return result
    }

    companion object {
      private val log = KotlinLogging.logger {}
    }
  }
