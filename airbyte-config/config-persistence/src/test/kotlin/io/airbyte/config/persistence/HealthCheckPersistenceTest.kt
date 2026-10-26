/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.data.services.HealthCheckService
import io.airbyte.data.services.impls.jooq.HealthCheckServiceJooqImpl
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.mockk.every
import io.mockk.mockk
import org.jooq.Result
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class HealthCheckPersistenceTest {
  private var database: Database? = null
  private var healthCheckService: HealthCheckService? = null

  @BeforeEach
  fun beforeEach() {
    database = mockk<Database>()
    healthCheckService = HealthCheckServiceJooqImpl(database)
  }

  @Test
  fun testHealthCheckSuccess() {
    val mResult = mockk<Result<*>>(relaxed = true)
    every { database!!.query<Any?>(any<ContextQueryFunction<Any?>>()) } returns mResult

    val check = healthCheckService!!.healthCheck()
    Assertions.assertTrue(check)
  }

  @Test
  fun testHealthCheckFailure() {
    every { database!!.query<Any?>(any<ContextQueryFunction<Any?>>()) } throws RuntimeException()

    val check = healthCheckService!!.healthCheck()
    Assertions.assertFalse(check)
  }
}
