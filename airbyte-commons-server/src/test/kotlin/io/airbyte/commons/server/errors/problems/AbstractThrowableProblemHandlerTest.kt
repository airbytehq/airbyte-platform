package io.airbyte.commons.server.errors.problems

import io.airbyte.api.problems.AbstractThrowableProblem
import io.airbyte.api.problems.ProblemResponse
import io.airbyte.api.problems.model.generated.BaseProblemFields
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Test

internal class AbstractThrowableProblemHandlerTest {
  private val abstractThrowableProblemHandler = AbstractThrowableProblemHandler()

  @Test
  fun testHandle() {
    val exception: AbstractThrowableProblem = TestThrowableProblem()

    val result = abstractThrowableProblemHandler.handle(null, exception)

    assert(result != null)
    assert(result?.status()?.code == 400)

    val parsedBody = Jsons.deserialize(result?.body() as String, BaseProblemFields::class.java)
    assert(parsedBody.status == 400)
    assert(parsedBody.title == "test-problem")
    assert(parsedBody.detail == "test-problem-detail")
    assert(parsedBody.type == "errors:problem/test")
    assert(parsedBody.documentationUrl == "https://airbyte.com/docs")
    assert(parsedBody.data == mapOf("test-key" to "test-val"))
  }

  companion object {
    class TestProblemResponse : ProblemResponse {
      override fun getStatus(): Int {
        return 400
      }

      override fun getTitle(): String {
        return "test-problem"
      }

      override fun getDetail(): String {
        return "test-problem-detail"
      }

      override fun getData(): Any {
        return mapOf("test-key" to "test-val")
      }

      override fun getType(): String {
        return "errors:problem/test"
      }

      override fun getDocumentationUrl(): String {
        return "https://airbyte.com/docs"
      }
    }

    class TestThrowableProblem : AbstractThrowableProblem(TestProblemResponse())
  }
}
