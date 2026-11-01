/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.problems

import io.airbyte.api.problems.AbstractThrowableProblem
import io.airbyte.api.problems.ProblemResponse
import io.airbyte.api.problems.model.generated.BaseProblemFields
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

internal class AbstractThrowableProblemHandlerTest {
  private val abstractThrowableProblemHandler = AbstractThrowableProblemHandler()

  @Test
  fun testHandle() {
    val exception: AbstractThrowableProblem = TestThrowableProblem()

    val result = abstractThrowableProblemHandler.handle(null, exception)

    assertNotNull(result)
    assertEquals(400, result?.status()?.code)

    val parsedBody = Jsons.deserialize(result?.body() as String, BaseProblemFields::class.java)
    assertEquals(400, parsedBody.status)
    assertEquals("test-problem", parsedBody.title)
    assertEquals("test-problem-detail", parsedBody.detail)
    assertEquals("errors:problem/test", parsedBody.type)
    assertEquals("https://airbyte.com/docs", parsedBody.documentationUrl)
    assertEquals(mapOf("test-key" to "test-val"), parsedBody.data)
  }

  companion object {
    class TestProblemResponse : ProblemResponse {
      override fun getStatus(): Int = 400

      override fun getTitle(): String = "test-problem"

      override fun getDetail(): String = "test-problem-detail"

      override fun getData(): Any = mapOf("test-key" to "test-val")

      override fun getType(): String = "errors:problem/test"

      override fun getDocumentationUrl(): String = "https://airbyte.com/docs"
    }

    class TestThrowableProblem : AbstractThrowableProblem(TestProblemResponse())
  }
}
