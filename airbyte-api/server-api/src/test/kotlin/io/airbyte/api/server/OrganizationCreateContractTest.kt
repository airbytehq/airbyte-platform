/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server

import com.fasterxml.jackson.databind.exc.MismatchedInputException
import io.airbyte.api.server.generated.models.OrganizationCreateRequestBody
import io.airbyte.commons.jackson.MoreMappers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class OrganizationCreateContractTest {
  private val mapper = MoreMappers.initMapper()

  @Test
  fun `organization create requires email`() {
    val userId = UUID.randomUUID()
    val withoutEmail = """{"userId":"$userId","organizationName":"Example"}"""
    val nullEmail = """{"userId":"$userId","organizationName":"Example","email":null}"""

    listOf(withoutEmail, nullEmail).forEach { body ->
      assertThrows<MismatchedInputException> {
        mapper.readValue(body, OrganizationCreateRequestBody::class.java)
      }
    }
  }

  @Test
  fun `organization create accepts email`() {
    val userId = UUID.randomUUID()
    val body = """{"userId":"$userId","organizationName":"Example","email":"owner@example.com"}"""

    val request = mapper.readValue(body, OrganizationCreateRequestBody::class.java)

    assertEquals("owner@example.com", request.email)
  }
}
