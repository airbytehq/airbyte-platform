/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE;

import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.BasicSchedule;
import io.airbyte.config.Schedule;
import io.airbyte.config.ScheduleData;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migration to make sure all rows have the new schedule data format. This was introduced back in
 * V0_36_3_001 and V0_38_4_001
 */
public class V0_50_11_001__CopyLegacyScheduleToNewScheduleData extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_11_001__CopyLegacyScheduleToNewScheduleData.class);

  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID);
  private static final Field<V0_36_3_001__AddScheduleTypeToConfigsTable.ScheduleType> SCHEDULE_TYPE_COLUMN = DSL.field(
      "schedule_type",
      SQLDataType.VARCHAR.asEnumDataType(V0_36_3_001__AddScheduleTypeToConfigsTable.ScheduleType.class).nullable(true));
  private static final Field<Boolean> MANUAL_COLUMN = DSL.field("manual", SQLDataType.BOOLEAN);
  private static final Field<JSONB> SCHEDULE_COLUMN = DSL.field("schedule", SQLDataType.JSONB);
  private static final Field<JSONB> SCHEDULE_DATA_COLUMN = DSL.field("schedule_data", SQLDataType.JSONB);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    migrateLegacyScheduleData(ctx);
  }

  static void migrateLegacyScheduleData(final DSLContext context) throws Exception {
    final var legacyScheduleConnections = context.select(List.of(ID_COLUMN, MANUAL_COLUMN, SCHEDULE_COLUMN))
        .from(CONNECTION_TABLE)
        .where(SCHEDULE_TYPE_COLUMN.isNull())
        .stream();
    legacyScheduleConnections.forEach(record -> {
      final Boolean isManual = record.getValue(MANUAL_COLUMN);
      context.update(DSL.table(CONNECTION_TABLE))
          .set(SCHEDULE_TYPE_COLUMN,
              isManual ? V0_36_3_001__AddScheduleTypeToConfigsTable.ScheduleType.manual
                  : V0_36_3_001__AddScheduleTypeToConfigsTable.ScheduleType.basicSchedule)
          .set(SCHEDULE_DATA_COLUMN, isManual ? null : legacyToScheduleData(record.getValue(SCHEDULE_COLUMN)))
          .where(ID_COLUMN.eq(record.get(ID_COLUMN)))
          .execute();
    });
  }

  private static JSONB legacyToScheduleData(final JSONB value) {
    // JSONB -> Schedule object.
    final var legacySchedule = Jsons.deserialize(value.data(), Schedule.class);
    return JSONB.valueOf(Jsons.serialize(new ScheduleData().withBasicSchedule(new BasicSchedule()
        .withTimeUnit(Enums.convertTo(legacySchedule.getTimeUnit(), BasicSchedule.TimeUnit.class))
        .withUnits(legacySchedule.getUnits()))));
  }

}
