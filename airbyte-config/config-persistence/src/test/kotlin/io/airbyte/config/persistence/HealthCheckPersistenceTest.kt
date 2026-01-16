/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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

internal class HealthCheckPersistenceTest {
  private var database: Database? = null
  private var healthCheckService: HealthCheckService? = null

  @BeforeEach
  fun beforeEach() {
    database = Mockito.mock<Database>(Database::class.java)
    healthCheckService = HealthCheckServiceJooqImpl(database)
  }

  @Test
  fun testHealthCheckSuccess() {
    val mResult = Mockito.mock<Result<*>?>(Result::class.java)
    Mockito.`when`<Any?>(database!!.query<Any?>(org.mockito.kotlin.any<ContextQueryFunction<Any?>>())).thenReturn(mResult)

    val check = healthCheckService!!.healthCheck()
    Assertions.assertTrue(check)
  }

  @Test
  fun testHealthCheckFailure() {
    Mockito.`when`<Any?>(database!!.query<Any?>(org.mockito.kotlin.any<ContextQueryFunction<Any?>>())).thenAnswer { throw RuntimeException() }

    val check = healthCheckService!!.healthCheck()
    Assertions.assertFalse(check)
  }
}
