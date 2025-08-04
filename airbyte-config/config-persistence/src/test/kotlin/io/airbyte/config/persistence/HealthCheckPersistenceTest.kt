/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.data.services.HealthCheckService
import io.airbyte.data.services.impls.jooq.HealthCheckServiceJooqImpl
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import org.jooq.Result
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.sql.SQLException

internal class HealthCheckPersistenceTest {
  private var database: Database? = null
  private var healthCheckService: HealthCheckService? = null

  @BeforeEach
  @Throws(Exception::class)
  fun beforeEach() {
    database = Mockito.mock<Database>(Database::class.java)
    healthCheckService = HealthCheckServiceJooqImpl(database)
  }

  @Test
  @Throws(SQLException::class)
  fun testHealthCheckSuccess() {
    val mResult = Mockito.mock<Result<*>?>(Result::class.java)
    Mockito.`when`<Any?>(database!!.query<Any?>(org.mockito.kotlin.any<ContextQueryFunction<Any?>>())).thenReturn(mResult)

    val check = healthCheckService!!.healthCheck()
    Assertions.assertTrue(check)
  }

  @Test
  @Throws(SQLException::class)
  fun testHealthCheckFailure() {
    Mockito.`when`<Any?>(database!!.query<Any?>(org.mockito.kotlin.any<ContextQueryFunction<Any?>>())).thenThrow(RuntimeException::class.java)

    val check = healthCheckService!!.healthCheck()
    Assertions.assertFalse(check)
  }
}
