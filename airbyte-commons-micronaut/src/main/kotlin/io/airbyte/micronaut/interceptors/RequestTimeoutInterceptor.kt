/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.interceptors

import dev.failsafe.Failsafe
import dev.failsafe.FailsafeException
import dev.failsafe.Timeout
import dev.failsafe.TimeoutExceededException
import io.airbyte.api.problems.throwable.generated.RequestTimeoutExceededProblem
import io.airbyte.micronaut.annotations.RequestTimeout
import io.micronaut.aop.InterceptorBean
import io.micronaut.aop.MethodInterceptor
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.core.annotation.AnnotationValue
import jakarta.inject.Singleton
import java.time.Duration

/**
 * AOP interceptor to ensure that the annotation method completes in the specified amount of time.
 * Methods marked with the [RequestTimeout] will be intercepted by this class.
 */
@Singleton
@InterceptorBean(RequestTimeout::class)
class RequestTimeoutInterceptor : MethodInterceptor<Any, Any> {
  override fun intercept(context: MethodInvocationContext<Any, Any>): Any? {
    val annotationValue = context.getAnnotation(RequestTimeout::class.java)
    return annotationValue?.let { doIntercept(it, context) } ?: context.proceed()
  }

  private fun doIntercept(
    annotationValue: AnnotationValue<RequestTimeout>,
    context: MethodInvocationContext<Any, Any>,
  ): Any {
    val timeoutValue = annotationValue.stringValue("timeout").get()
    val duration = Duration.parse(timeoutValue)
    val timeout = Timeout.builder<Any>(duration).withInterrupt().build()
    return try {
      Failsafe.with(timeout).get(context::proceed)
    } catch (e: FailsafeException) {
      when (e) {
        is TimeoutExceededException -> throw RequestTimeoutExceededProblem(detail = e.message, data = mapOf("timeout" to timeoutValue))
        // Unwrap the failsafe exception if not due to timeout
        else -> throw e.cause ?: e
      }
    }
  }
}
