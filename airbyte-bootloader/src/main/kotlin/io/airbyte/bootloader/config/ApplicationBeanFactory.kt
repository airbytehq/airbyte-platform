/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.config

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

/**
 * Micronaut bean factory for general application-related singletons.
 */
@Factory
class ApplicationBeanFactory {
  @Singleton
  fun kubernetesClient(): KubernetesClient = KubernetesClientBuilder().build()
}
