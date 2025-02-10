/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.interceptors

import io.airbyte.api.problems.throwable.generated.RequestTimeoutExceededProblem
import io.airbyte.micronaut.annotations.RequestTimeout
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.lang.Thread.sleep
import java.time.Duration

internal class RequestTimeoutInterceptorTest {
  @Test
  fun testRequestTimeout() {
    val applicationContext = ApplicationContext.run()
    val exampleBean = applicationContext.getBean(RequestTimeoutExample::class.java)
    val e =
      assertThrows<RequestTimeoutExceededProblem> {
        exampleBean.functionOne("a test string")
      }
    assertEquals(mapOf("timeout" to "PT1S"), e.problem.data)
    assertEquals(HttpStatus.REQUEST_TIMEOUT.code, e.problem.status)
    assertEquals("The request has exceeded the timeout associated with this endpoint.", e.problem.detail)
    assertEquals("error:request-timeout-exceeded", e.problem.type)
    assertEquals("Request timeout exceeded", e.problem.title)
    applicationContext.close()
  }

  @Test
  fun testNoRequestTimeout() {
    val applicationContext = ApplicationContext.run()
    val exampleBean = applicationContext.getBean(RequestTimeoutExample::class.java)
    assertDoesNotThrow {
      exampleBean.functionTwo("a test string")
    }
    applicationContext.close()
  }

  @Test
  fun testFunctionThrowsBeforeTimeout() {
    val applicationContext = ApplicationContext.run()
    val exampleBean = applicationContext.getBean(RequestTimeoutExample::class.java)
    assertThrows<IOException> {
      exampleBean.functionThree()
    }
    applicationContext.close()
  }
}

@Singleton
internal open class RequestTimeoutExample {
  @RequestTimeout(timeout = "PT1S")
  open fun functionOne(with: String) {
    sleep(Duration.ofSeconds(2))
    println("Doing something with $with")
  }

  @RequestTimeout(timeout = "PT2S")
  open fun functionTwo(with: String): Unit = println("Doing something with $with")

  @RequestTimeout(timeout = "PT2S")
  open fun functionThree(): Unit = throw IOException("oops")
}
