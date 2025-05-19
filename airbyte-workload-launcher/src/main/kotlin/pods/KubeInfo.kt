/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

/**
 * POJO for Kube container info.
 *
 * @param image image
 * @param pullPolicy pull policy
 */
data class KubeContainerInfo(
  val image: String,
  val pullPolicy: String,
)

/**
 * Kube pod info.
 *
 * @param namespace namespace
 * @param name pod name
 * @param mainContainerInfo container info
 */
data class KubePodInfo(
  val namespace: String?,
  val name: String,
  val mainContainerInfo: KubeContainerInfo?,
)
