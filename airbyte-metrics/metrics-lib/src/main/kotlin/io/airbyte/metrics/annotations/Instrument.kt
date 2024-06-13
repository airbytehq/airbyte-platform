package io.airbyte.metrics.annotations

import io.micronaut.aop.Around

annotation class Tag(val key: String, val value: String)

/**
 * Set this annotation to a function to instrument metric emissions.
 *
 * As we are using [io.airbyte.metrics.lib.MetricsRegistry] to define the metrics, values provided for [start], [end] and [duration] need to be
 * valid values from a [io.airbyte.metrics.lib.MetricsRegistry].
 *
 * For the [end] and [duration] metric, a `status` tag with values `ok` or `error` will be automatically added. Status being `error` if an
 * exception is thrown, `ok` otherwise.
 *
 * @property start if not empty defines the name of metric to emit on start of the method.
 * @property end if not empty defines the name metric to emit at the end of the method.
 * @property duration if not empty defines the name metric to the duration of the method.
 * @property tags defines a list of custom tags to be added to each metrics.
 */
@MustBeDocumented
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FUNCTION)
@Around
annotation class Instrument(
  val start: String = "",
  val end: String = "",
  val duration: String = "",
  val tags: Array<Tag> = [],
)
