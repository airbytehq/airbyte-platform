/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigSchema
import io.airbyte.config.Configs
import io.airbyte.config.StandardSyncState
import io.airbyte.config.State
import io.airbyte.db.Database
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.Companion.copyData
import io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.Companion.getJobsDatabase
import io.airbyte.db.instance.configs.migrations.V0_30_22_001__Store_last_sync_state.Companion.getStandardSyncState
import io.airbyte.db.instance.jobs.JobsDatabaseTestProvider
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.Timeout
import org.mockito.Mockito
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.TimeUnit

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_30_22_001__Store_last_sync_state_test : AbstractConfigsDatabaseTest() {
  private lateinit var jobDatabase: Database

  @BeforeEach
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  fun setupJobDatabase() {
    jobDatabase = JobsDatabaseTestProvider(dslContext, null).create(false)
  }

  @Test
  @Order(10)
  fun testGetJobsDatabase() {
    Assertions.assertNull(getJobsDatabase("", "", ""))

    // when there is database environment variable, return the database
    val configs = Mockito.mock(Configs::class.java)
    Mockito.`when`(configs.databaseUser).thenReturn(container.username)
    Mockito.`when`(configs.databasePassword).thenReturn(container.password)
    Mockito.`when`(configs.databaseUrl).thenReturn(container.jdbcUrl)

    Assertions.assertNotNull(
      getJobsDatabase(configs.databaseUser, configs.databasePassword, configs.databaseUrl),
    )
  }

  @Test
  @Order(30)
  @Throws(SQLException::class)
  fun testCopyData() {
    val newConnectionStates =
      setOf(
        StandardSyncState()
          .withConnectionId(CONNECTION_2_ID)
          .withState(State().withState(Jsons.deserialize("""{ "cursor": 3 }"""))),
      )

    val timestampWithFullPrecision = OffsetDateTime.now()
        /*
         * The AWS CI machines get a higher precision value here (2021-12-07T19:56:28.967213187Z) vs what is
         * retrievable on Postgres or on my local machine (2021-12-07T19:56:28.967213Z). Truncating the
         * value to match.
         */
    val timestamp = timestampWithFullPrecision.withNano(1000 * (timestampWithFullPrecision.nano / 1000))

    jobDatabase.query<Any?> { ctx: DSLContext ->
      copyData(
        ctx,
        STD_CONNECTION_STATES,
        timestamp,
      )
      checkSyncStates(
        ctx,
        STD_CONNECTION_STATES,
        timestamp,
      )

      // call the copyData method again with different data will not affect existing records
      copyData(ctx, newConnectionStates, OffsetDateTime.now())
      // the states remain the same as those in STD_CONNECTION_STATES
      checkSyncStates(
        ctx,
        STD_CONNECTION_STATES,
        timestamp,
      )
      null
    }
  }

  companion object {
    private val CONNECTION_2_ID: UUID = UUID.randomUUID()
    private val CONNECTION_3_ID: UUID = UUID.randomUUID()

    private val CONNECTION_2_STATE: State = Jsons.deserialize("""{ "state": { "cursor": 2222 } }""", State::class.java)
    private val CONNECTION_3_STATE: State = Jsons.deserialize("""{ "state": { "cursor": 3333 } }""", State::class.java)
    private val STD_CONNECTION_STATE_2 = getStandardSyncState(CONNECTION_2_ID, CONNECTION_2_STATE)
    private val STD_CONNECTION_STATE_3 = getStandardSyncState(CONNECTION_3_ID, CONNECTION_3_STATE)
    private val STD_CONNECTION_STATES: Set<StandardSyncState> = setOf(STD_CONNECTION_STATE_2, STD_CONNECTION_STATE_3)

    private fun checkSyncStates(
      ctx: DSLContext,
      standardSyncStates: Set<StandardSyncState>,
      expectedTimestamp: OffsetDateTime?,
    ) {
      for (standardSyncState in standardSyncStates) {
        val record =
          ctx
            .select(
              V0_30_22_001__Store_last_sync_state.COLUMN_CONFIG_BLOB,
              V0_30_22_001__Store_last_sync_state.COLUMN_CREATED_AT,
              V0_30_22_001__Store_last_sync_state.COLUMN_UPDATED_AT,
            ).from(V0_30_22_001__Store_last_sync_state.TABLE_AIRBYTE_CONFIGS)
            .where(
              V0_30_22_001__Store_last_sync_state.COLUMN_CONFIG_ID.eq(standardSyncState.connectionId.toString()),
              V0_30_22_001__Store_last_sync_state.COLUMN_CONFIG_TYPE.eq(ConfigSchema.STANDARD_SYNC_STATE.name),
            ).fetchOne()
        Assertions.assertEquals(
          standardSyncState,
          Jsons.deserialize(
            record!!.value1().data(),
            StandardSyncState::class.java,
          ),
        )
        if (expectedTimestamp != null) {
          Assertions.assertEquals(expectedTimestamp, record.value2())
          Assertions.assertEquals(expectedTimestamp, record.value3())
        }
      }
    }
  }
}
