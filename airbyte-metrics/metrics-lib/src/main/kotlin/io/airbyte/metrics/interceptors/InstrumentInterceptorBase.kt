package io.airbyte.metrics.interceptors

import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricAttribute
import io.micronaut.aop.MethodInterceptor
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.core.annotation.AnnotationValue
import kotlin.time.Duration
import kotlin.time.TimeSource

abstract class InstrumentInterceptorBase : MethodInterceptor<Any, Any> {
  companion object {
    const val START = "start"
    const val END = "end"
    const val DURATION = "duration"
    const val TAGS = "tags"

    const val SUCCESS_STATUS = "ok"
    const val FAILURE_STATUS = "error"
  }

  abstract fun emitStartMetric(
    startMetricName: String,
    tags: Array<MetricAttribute>,
  )

  abstract fun emitEndMetric(
    endMetricName: String,
    success: Boolean,
    tags: Array<MetricAttribute>,
  )

  abstract fun emitDurationMetric(
    durationMetricName: String,
    duration: Duration,
    success: Boolean,
    tags: Array<MetricAttribute>,
  )

  override fun intercept(context: MethodInvocationContext<Any, Any>): Any? {
    val annotationValue = context.getAnnotation(Instrument::class.java)
    return if (annotationValue != null) {
      doIntercept(annotationValue, context)
    } else {
      context.proceed()
    }
  }

  private fun doIntercept(
    annotationValue: AnnotationValue<Instrument>,
    context: MethodInvocationContext<Any, Any>,
  ): Any? {
    val tags = readTags(annotationValue)

    annotationValue.stringValue(START).ifPresent { emitStartMetric(it, tags) }

    var success = true
    val startTime = TimeSource.Monotonic.markNow()
    try {
      return context.proceed()
    } catch (e: Throwable) {
      success = false
      throw e
    } finally {
      annotationValue.stringValue(END).ifPresent { emitEndMetric(it, success, tags) }
      annotationValue.stringValue(DURATION).ifPresent { emitDurationMetric(it, startTime.elapsedNow(), success, tags) }
    }
  }

  private fun readTags(annotationValue: AnnotationValue<Instrument>): Array<MetricAttribute> {
    return annotationValue.getAnnotations<Tag>(TAGS)
      .map { MetricAttribute(it.stringValue("key").orElse(""), it.stringValue("value").orElse("")) }
      .toTypedArray()
  }
}
