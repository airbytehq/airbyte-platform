/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.config.persistence.WorkloadPurge
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.EnableWorkloadPurge
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WorkloadPurgeRunRequest
import io.airbyte.metrics.MetricClient
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

internal class WorkloadPurgeLifecycleTest {
  private val purge = mockk<WorkloadPurge>()
  private val metrics = mockk<MetricClient>(relaxed = true)
  private val featureFlags = mockk<FeatureFlagClient>()
  private var requestToken = ""

  @Test
  fun `request arriving after application startup is not mistaken for historical token`() {
    withStartedContext { context ->
      requestToken = "new-request"

      context.getBean(WorkloadPurgeWorkflow::class.java).pollRunRequests()

      verify(exactly = 1) { purge.prune(any()) }
    }
  }

  @Test
  fun `historical token present during application startup remains ignored`() {
    requestToken = "historical-request"

    withStartedContext { context ->
      context.getBean(WorkloadPurgeWorkflow::class.java).pollRunRequests()

      verify(exactly = 0) { purge.prune(any()) }
    }
  }

  private fun withStartedContext(action: (ApplicationContext) -> Unit) {
    every { featureFlags.stringVariation(WorkloadPurgeRunRequest, Empty) } answers { requestToken }
    every { featureFlags.boolVariation(EnableWorkloadPurge, Empty) } returns true
    every { purge.prune(any()) } returns 0
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("workload-purge-test-isolation") {
        override fun isWithin(beanDefinitionReference: BeanDefinitionReference<*>): Boolean =
          isWithin(beanDefinitionReference.beanDefinitionName.replace("$", ""))

        override fun isWithin(cls: Class<*>): Boolean = isWithin(cls.name)

        override fun isWithin(className: String): Boolean =
          className.startsWith("io.airbyte.") && !className.replace("$", "").startsWith(WorkloadPurgeWorkflow::class.java.name)
      }
    ApplicationContext
      .builder()
      .enableDefaultPropertySources(false)
      .deduceEnvironment(false)
      .environmentPropertySource(false)
      .beanConfigurations(isolated)
      .build()
      .use { context ->
        context.registerSingleton(WorkloadPurge::class.java, purge)
        context.registerSingleton(MetricClient::class.java, metrics)
        context.registerSingleton(FeatureFlagClient::class.java, featureFlags)
        context.start()
        action(context)
      }
  }
}
