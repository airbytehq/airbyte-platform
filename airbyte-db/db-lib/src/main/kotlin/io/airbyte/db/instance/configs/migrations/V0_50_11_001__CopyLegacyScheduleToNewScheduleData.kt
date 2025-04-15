/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.enums.Enums
import io.airbyte.commons.json.Jsons
import io.airbyte.config.BasicSchedule
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.stream.Stream

private val log = KotlinLogging.logger {}

/**
 * Migration to make sure all rows have the new schedule data format. This was introduced back in V0_36_3_001 and V0_38_4_001
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_11_001__CopyLegacyScheduleToNewScheduleData : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    migrateLegacyScheduleData(ctx)
  }

  companion object {
    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID)
    private val SCHEDULE_TYPE_COLUMN =
      DSL.field(
        "schedule_type",
        SQLDataType.VARCHAR.asEnumDataType(V0_36_3_001__AddScheduleTypeToConfigsTable.ScheduleType::class.java).nullable(true),
      )
    private val MANUAL_COLUMN = DSL.field("manual", SQLDataType.BOOLEAN)
    private val SCHEDULE_COLUMN = DSL.field("schedule", SQLDataType.JSONB)
    private val SCHEDULE_DATA_COLUMN = DSL.field("schedule_data", SQLDataType.JSONB)

    fun migrateLegacyScheduleData(context: DSLContext) {
      val legacyScheduleConnections: Stream<Record> =
        context
          .select(listOf(ID_COLUMN, MANUAL_COLUMN, SCHEDULE_COLUMN))
          .from(CONNECTION_TABLE)
          .where(SCHEDULE_TYPE_COLUMN.isNull())
          .stream()
      legacyScheduleConnections.forEach { record: Record ->
        val isManual = record.getValue(MANUAL_COLUMN)
        context
          .update(DSL.table(CONNECTION_TABLE))
          .set(
            SCHEDULE_TYPE_COLUMN,
            if (isManual) {
              V0_36_3_001__AddScheduleTypeToConfigsTable.ScheduleType.manual
            } else {
              V0_36_3_001__AddScheduleTypeToConfigsTable.ScheduleType.basicSchedule
            },
          ).set(
            SCHEDULE_DATA_COLUMN,
            if (isManual) {
              null
            } else {
              legacyToScheduleData(
                record.getValue(SCHEDULE_COLUMN),
              )
            },
          ).where(ID_COLUMN.eq(record.get(ID_COLUMN)))
          .execute()
      }
    }

    private fun legacyToScheduleData(value: JSONB): JSONB {
      // JSONB -> Schedule object.
      val legacySchedule = Jsons.deserialize(value.data(), Schedule::class.java)
      return JSONB.valueOf(
        Jsons.serialize(
          ScheduleData().withBasicSchedule(
            BasicSchedule()
              .withTimeUnit(
                Enums.convertTo(
                  legacySchedule.timeUnit,
                  BasicSchedule.TimeUnit::class.java,
                ),
              ).withUnits(legacySchedule.units),
          ),
        ),
      )
    }
  }
}
