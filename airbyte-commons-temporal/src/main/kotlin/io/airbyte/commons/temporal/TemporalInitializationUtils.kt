/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.micronaut.runtime.AirbyteTemporalConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.StatusRuntimeException
import io.temporal.api.workflowservice.v1.DescribeNamespaceRequest
import io.temporal.serviceclient.WorkflowServiceStubs
import jakarta.inject.Singleton
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

/**
 * Utils for verifying that temporal is running and available. Used at the startup of an
 * application.
 */
@Singleton
class TemporalInitializationUtils(
  private val temporalService: WorkflowServiceStubs,
  private val airbyteTemporalConfig: AirbyteTemporalConfig,
) {
  /**
   * Blocks until the Temporal [TemporalUtils.DEFAULT_NAMESPACE] has been created. This is
   * necessary to avoid issues related to
   * https://community.temporal.io/t/running-into-an-issue-when-creating-namespace-programmatically/2783/8.
   */
  fun waitForTemporalNamespace() {
    var namespaceExists = false
    val temporalNamespace = getTemporalNamespace()
    while (!namespaceExists) {
      try {
        // This is to allow the configured namespace to be available in the Temporal
        // cache before continuing on with any additional configuration/bean creation.
        logger.info { "temporal service : $temporalService" }
        logger.info { "temporal namespace : $temporalNamespace" }
        temporalService.blockingStub().describeNamespace(DescribeNamespaceRequest.newBuilder().setNamespace(temporalNamespace).build())
        namespaceExists = true
        // This is to allow the configured namespace to be available in the Temporal
        // cache before continuing on with any additional configuration/bean creation.
        Thread.sleep(TimeUnit.SECONDS.toMillis(5))
      } catch (e: InterruptedException) {
        logger.debug(e) { "Namespace '$temporalNamespace' does not exist yet.  Re-checking..." }
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(5))
        } catch (ie: InterruptedException) {
          logger.debug(ie) { "Sleep interrupted.  Exiting loop..." }
        }
      } catch (e: StatusRuntimeException) {
        logger.debug(e) { "Namespace '$temporalNamespace' does not exist yet.  Re-checking..." }
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(5))
        } catch (ie: InterruptedException) {
          logger.debug(ie) { "Sleep interrupted.  Exiting loop..." }
        }
      }
    }
  }

  /**
   * Retrieve the Temporal namespace based on the configuration.
   *
   * @return The Temporal namespace.
   */
  private fun getTemporalNamespace(): String =
    airbyteTemporalConfig.cloud.namespace.ifBlank {
      TemporalUtils.DEFAULT_NAMESPACE
    }
}
