/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.COLUMN_CONFIG_BLOB;
import static io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.COLUMN_CONFIG_ID;
import static io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.COLUMN_CONFIG_TYPE;
import static io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.COLUMN_CREATED_AT;
import static io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.COLUMN_UPDATED_AT;
import static io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.TABLE_AIRBYTE_CONFIGS;
import static io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.getStandardSyncState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Configs;
import io.airbyte.config.StandardSyncState;
import io.airbyte.config.State;
import io.airbyte.db.Database;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.jobs.JobsDatabaseTestProvider;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class V0_30_22_001__Store_last_sync_state_test extends AbstractConfigsDatabaseTest {

  private static final UUID CONNECTION_2_ID = UUID.randomUUID();
  private static final UUID CONNECTION_3_ID = UUID.randomUUID();

  private static final State CONNECTION_2_STATE = Jsons.deserialize("{ \"state\": { \"cursor\": 2222 } }", State.class);
  private static final State CONNECTION_3_STATE = Jsons.deserialize("{ \"state\": { \"cursor\": 3333 } }", State.class);

  private static final StandardSyncState STD_CONNECTION_STATE_2 = getStandardSyncState(CONNECTION_2_ID, CONNECTION_2_STATE);
  private static final StandardSyncState STD_CONNECTION_STATE_3 = getStandardSyncState(CONNECTION_3_ID, CONNECTION_3_STATE);
  private static final Set<StandardSyncState> STD_CONNECTION_STATES = Set.of(STD_CONNECTION_STATE_2, STD_CONNECTION_STATE_3);

  private Database jobDatabase;

  @BeforeEach
  @Timeout(value = 2,
           unit = TimeUnit.MINUTES)
  void setupJobDatabase() throws DatabaseInitializationException, IOException {
    jobDatabase = new JobsDatabaseTestProvider(dslContext, null).create(false);
  }

  @Test
  @Order(10)
  void testGetJobsDatabase() {
    assertTrue(V0_30_22_001__Store_last_sync_state.getJobsDatabase("", "", "").isEmpty());

    // when there is database environment variable, return the database
    final Configs configs = mock(Configs.class);
    when(configs.getDatabaseUser()).thenReturn(container.getUsername());
    when(configs.getDatabasePassword()).thenReturn(container.getPassword());
    when(configs.getDatabaseUrl()).thenReturn(container.getJdbcUrl());

    assertTrue(V0_30_22_001__Store_last_sync_state
        .getJobsDatabase(configs.getDatabaseUser(), configs.getDatabasePassword(), configs.getDatabaseUrl()).isPresent());
  }

  @Test
  @Order(30)
  void testCopyData() throws SQLException {

    final Set<StandardSyncState> newConnectionStates = Collections.singleton(
        new StandardSyncState()
            .withConnectionId(CONNECTION_2_ID)
            .withState(new State().withState(Jsons.deserialize("{ \"cursor\": 3 }"))));

    final OffsetDateTime timestampWithFullPrecision = OffsetDateTime.now();
    /*
     * The AWS CI machines get a higher precision value here (2021-12-07T19:56:28.967213187Z) vs what is
     * retrievable on Postgres or on my local machine (2021-12-07T19:56:28.967213Z). Truncating the
     * value to match.
     */
    final OffsetDateTime timestamp = timestampWithFullPrecision.withNano(1000 * (timestampWithFullPrecision.getNano() / 1000));

    jobDatabase.query(ctx -> {
      V0_30_22_001__Store_last_sync_state.copyData(ctx, STD_CONNECTION_STATES, timestamp);
      checkSyncStates(ctx, STD_CONNECTION_STATES, timestamp);

      // call the copyData method again with different data will not affect existing records
      V0_30_22_001__Store_last_sync_state.copyData(ctx, newConnectionStates, OffsetDateTime.now());
      // the states remain the same as those in STD_CONNECTION_STATES
      checkSyncStates(ctx, STD_CONNECTION_STATES, timestamp);

      return null;
    });
  }

  private static void checkSyncStates(final DSLContext ctx,
                                      final Set<StandardSyncState> standardSyncStates,
                                      @Nullable final OffsetDateTime expectedTimestamp) {
    for (final StandardSyncState standardSyncState : standardSyncStates) {
      final var record = ctx
          .select(COLUMN_CONFIG_BLOB,
              COLUMN_CREATED_AT,
              COLUMN_UPDATED_AT)
          .from(TABLE_AIRBYTE_CONFIGS)
          .where(COLUMN_CONFIG_ID.eq(standardSyncState.getConnectionId().toString()),
              COLUMN_CONFIG_TYPE.eq(ConfigSchema.STANDARD_SYNC_STATE.name()))
          .fetchOne();
      assertEquals(standardSyncState, Jsons.deserialize(record.value1().data(), StandardSyncState.class));
      if (expectedTimestamp != null) {
        assertEquals(expectedTimestamp, record.value2());
        assertEquals(expectedTimestamp, record.value3());
      }
    }
  }

}
