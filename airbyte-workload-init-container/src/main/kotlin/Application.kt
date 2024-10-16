/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.initContainer

import io.airbyte.commons.envvar.EnvVar
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.ApplicationContext
import org.apache.commons.lang3.time.StopWatch

private val logger = KotlinLogging.logger {}

fun main() {
  val initContainerPerformancesMeasurement = StopWatch()
  initContainerPerformancesMeasurement.start()
  ApplicationContext.run().use {
    logger.info { "Init start" }
    logger.info { "Application context started at: ${initContainerPerformancesMeasurement.time}" }

    val workloadId = EnvVar.WORKLOAD_ID.fetch()!!
    logger.info { "Workload Id fetched at: ${initContainerPerformancesMeasurement.time}" }

    val fetcher = it?.getBean(InputFetcher::class.java)
    logger.info { "Fetcher bean retrieved at: ${initContainerPerformancesMeasurement.time}" }

    fetcher?.fetch(workloadId, initContainerPerformancesMeasurement)

    logger.info { "Workload fetch at: ${initContainerPerformancesMeasurement.time}" }
    logger.info { "Init end" }
    initContainerPerformancesMeasurement.stop()
  }
}
