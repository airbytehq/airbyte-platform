/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.db.Database
import io.airbyte.db.instance.configs.jooq.generated.Tables
import org.jooq.DSLContext
import org.jooq.impl.DSL
import java.util.UUID

class DataplaneGroupServiceTestJooqImpl(
  private val database: Database,
) : DataplaneGroupService {
  override fun writeDataplaneGroup(dataplaneGroup: DataplaneGroup): DataplaneGroup {
    database.transaction<Any?> { ctx: DSLContext ->
      val isExistingConfig =
        ctx.fetchExists(
          DSL
            .select()
            .from(Tables.DATAPLANE_GROUP)
            .where(Tables.DATAPLANE_GROUP.ID.eq(dataplaneGroup.id)),
        )
      if (isExistingConfig) {
        ctx
          .update(Tables.DATAPLANE_GROUP)
          .set(Tables.DATAPLANE_GROUP.ORGANIZATION_ID, dataplaneGroup.organizationId)
          .set(Tables.DATAPLANE_GROUP.NAME, dataplaneGroup.name)
          .set(Tables.DATAPLANE_GROUP.ENABLED, dataplaneGroup.enabled)
          .set(Tables.DATAPLANE_GROUP.TOMBSTONE, dataplaneGroup.tombstone)
          .execute()
      } else {
        ctx
          .insertInto(Tables.DATAPLANE_GROUP)
          .set(Tables.DATAPLANE_GROUP.ID, dataplaneGroup.id)
          .set(Tables.DATAPLANE_GROUP.ORGANIZATION_ID, dataplaneGroup.organizationId)
          .set(Tables.DATAPLANE_GROUP.NAME, dataplaneGroup.name)
          .set(Tables.DATAPLANE_GROUP.ENABLED, dataplaneGroup.enabled)
          .set(Tables.DATAPLANE_GROUP.TOMBSTONE, dataplaneGroup.tombstone)
          .execute()
      }
    }
    return dataplaneGroup
  }

  override fun getDataplaneGroup(id: UUID): DataplaneGroup =
    database.query { ctx: DSLContext ->
      ctx
        .selectFrom(Tables.DATAPLANE_GROUP)
        .where(Tables.DATAPLANE_GROUP.ID.eq(id))
        .fetchOneInto(DataplaneGroup::class.java)
        ?: throw ConfigNotFoundException(DataplaneGroup::class.toString(), id.toString())
    }

  override fun getDataplaneGroupByOrganizationIdAndName(
    organizationId: UUID,
    name: String,
  ): DataplaneGroup {
    val result =
      database
        .query({ ctx: DSLContext ->
          ctx
            .select(
              Tables.DATAPLANE_GROUP.asterisk(),
            ).from(Tables.DATAPLANE_GROUP)
            .where(Tables.DATAPLANE_GROUP.ORGANIZATION_ID.eq(organizationId))
            .and(Tables.DATAPLANE_GROUP.NAME.equalIgnoreCase(name))
        })
        .fetch()
    return result.first().into(DataplaneGroup::class.java)
  }

  override fun listDataplaneGroups(
    organizationIds: List<UUID>,
    withTombstone: Boolean,
  ): List<DataplaneGroup> =
    database.query { ctx: DSLContext ->
      var condition = Tables.DATAPLANE_GROUP.ORGANIZATION_ID.`in`(organizationIds)

      if (!withTombstone) {
        condition = condition.and(Tables.DATAPLANE_GROUP.TOMBSTONE.eq(false))
      }
      ctx
        .selectFrom(Tables.DATAPLANE_GROUP)
        .where(condition)
        .fetchInto(DataplaneGroup::class.java)
    }

  // Not needed for test implementation
  override fun getDefaultDataplaneGroupForAirbyteEdition(airbyteEdition: AirbyteEdition): DataplaneGroup =
    throw UnsupportedOperationException("Not implemented for test service")

  override fun getOrganizationIdFromDataplaneGroup(dataplaneGroupId: UUID): UUID =
    database.query { ctx: DSLContext ->
      ctx
        .select(Tables.DATAPLANE_GROUP.ORGANIZATION_ID)
        .from(Tables.DATAPLANE_GROUP)
        .where(Tables.DATAPLANE_GROUP.ID.eq(dataplaneGroupId))
        .fetchOneInto(UUID::class.java)
        ?: throw ConfigNotFoundException(DataplaneGroup::class.toString(), dataplaneGroupId.toString())
    }
}
