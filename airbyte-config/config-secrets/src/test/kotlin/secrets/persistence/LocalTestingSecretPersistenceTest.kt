/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import io.airbyte.config.secrets.SecretCoordinate
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.RowCountQuery
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class LocalTestingSecretPersistenceTest {
  @Test
  fun `test reading secret from database`() {
    val secret = "secret value"
    val coordinate = SecretCoordinate.fromFullCoordinate("secret_coordinate_v1")
    val mockDslContext: DSLContext = mockk()
    val mockResult: org.jooq.Result<Record> = mockk()
    val mockRecord: Record = mockk()

    every { mockRecord.getValue(0, String::class.java) } returns secret
    every { mockResult.size } returns 1
    every { mockResult[0] } returns mockRecord
    every { mockDslContext.execute(any<String>()) } returns 1
    every { mockDslContext.fetch(any<String>(), any<String>()) } returns mockResult

    val persistence = LocalTestingSecretPersistence(mockDslContext)
    val result = persistence.read(coordinate)

    Assertions.assertEquals(secret, result)
  }

  @Test
  fun `test reading missing secret from database`() {
    val coordinate = SecretCoordinate.fromFullCoordinate("secret_coordinate_v1")
    val mockDslContext: DSLContext = mockk()
    val mockResult: org.jooq.Result<Record> = mockk()
    val mockRecord: Record = mockk()

    every { mockRecord.getValue(0, String::class.java) } returns ""
    every { mockResult.size } returns 1
    every { mockResult[0] } returns mockRecord
    every { mockDslContext.execute(any<String>()) } returns 1
    every { mockDslContext.fetch(any<String>(), any<String>()) } returns mockResult

    val persistence = LocalTestingSecretPersistence(mockDslContext)
    val result = persistence.read(coordinate)

    Assertions.assertEquals("", result)
  }

  @Test
  fun `test reading from database with no result`() {
    val coordinate = SecretCoordinate.fromFullCoordinate("secret_coordinate_v1")
    val mockDslContext: DSLContext = mockk()
    val mockResult: org.jooq.Result<Record> = mockk()
    val mockRecord: Record = mockk()

    every { mockRecord.getValue(0, String::class.java) } returns ""
    every { mockResult.size } returns 0
    every { mockDslContext.execute(any<String>()) } returns 1
    every { mockDslContext.fetch(any<String>(), any<String>()) } returns mockResult

    val persistence = LocalTestingSecretPersistence(mockDslContext)
    val result = persistence.read(coordinate)

    Assertions.assertEquals("", result)
  }

  @Test
  fun `test writing a secret to the database`() {
    val secret = "a secret value"
    val coordinate = SecretCoordinate.fromFullCoordinate("secret_coordinate_v1")
    val mockDslContext: DSLContext = mockk()
    val mockRowCountQuery: RowCountQuery = mockk()

    every { mockRowCountQuery.execute() } returns 1
    every { mockDslContext.execute(any<String>()) } returns 1
    every { mockDslContext.query(any<String>(), any<String>(), any<String>(), any<String>(), any<String>()) } returns mockRowCountQuery

    val persistence = LocalTestingSecretPersistence(mockDslContext)

    persistence.write(coordinate, secret)

    verify { mockRowCountQuery.execute() }
  }
}
