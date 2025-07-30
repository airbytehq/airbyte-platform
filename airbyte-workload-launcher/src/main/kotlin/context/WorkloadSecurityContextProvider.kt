/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.context

import io.fabric8.kubernetes.api.model.CapabilitiesBuilder
import io.fabric8.kubernetes.api.model.PodSecurityContext
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder
import io.fabric8.kubernetes.api.model.SeccompProfileBuilder
import io.fabric8.kubernetes.api.model.SecurityContext
import io.fabric8.kubernetes.api.model.SecurityContextBuilder
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

val DEFAULT_CAPABILITIES = listOf("ALL")
const val ROOT_USER_ID = 0L
const val ROOTLESS_USER_ID = 1000L
const val ROOTLESS_GROUP_ID = 1000L
const val SECCOMP_PROFILE_TYPE = "RuntimeDefault"

@Singleton
class WorkloadSecurityContextProvider(
  @Value("\${airbyte.container.rootless-workload}") private val rootlessWorkload: Boolean,
) {
  /**
   * Returns a default [io.fabric8.kubernetes.api.model.SecurityContext] specific to containers.
   *
   * @return SecurityContext if ROOTLESS_WORKLOAD is enabled, null otherwise.
   */
  fun defaultContainerSecurityContext(
    user: Long,
    group: Long,
  ): SecurityContext? {
    if (rootlessWorkload) {
      return baseContainerSecurityContext(user = user, group = group).build()
    }

    return null
  }

  /**
   * Returns a rootless [SecurityContext] specific to containers.
   *
   * @return SecurityContext if ROOTLESS_WORKLOAD is enabled, null otherwise.
   */
  fun rootlessContainerSecurityContext(): SecurityContext? =
    when (rootlessWorkload) {
      true ->
        baseContainerSecurityContext(user = ROOTLESS_USER_ID, group = ROOTLESS_GROUP_ID)
          .withRunAsNonRoot(true)
          .withSeccompProfile(SeccompProfileBuilder().withType(SECCOMP_PROFILE_TYPE).build())
          .build()
      false -> null
    }

  /**
   * Returns a rootless [io.fabric8.kubernetes.api.model.PodSecurityContext] specific to pods.
   *
   * @return SecurityContext if ROOTLESS_WORKLOAD is enabled, null otherwise.
   */
  fun rootlessPodSecurityContext(
    user: Long,
    group: Long,
  ): PodSecurityContext? =
    when (rootlessWorkload) {
      true ->
        PodSecurityContextBuilder()
          .withRunAsUser(user)
          .withRunAsGroup(group)
          .withFsGroup(group)
          .withRunAsNonRoot(true)
          .withSeccompProfile(SeccompProfileBuilder().withType(SECCOMP_PROFILE_TYPE).build())
          .build()
      false -> null
    }

  /**
   * Returns a default [PodSecurityContext] specific to pods.
   *
   * @return SecurityContext if ROOTLESS_WORKLOAD is enabled, null otherwise.
   */
  fun defaultPodSecurityContext(): PodSecurityContext? =
    when (rootlessWorkload) {
      true -> PodSecurityContextBuilder().withFsGroup(ROOTLESS_GROUP_ID).build()
      false -> null
    }

  fun socketRootlessPodSecurityContext(
    user: Long = ROOTLESS_USER_ID,
    group: Long = ROOTLESS_GROUP_ID,
  ): PodSecurityContext =
    PodSecurityContextBuilder()
      .withRunAsUser(user)
      .withRunAsGroup(group)
      .withFsGroup(group)
      .withFsGroupChangePolicy("OnRootMismatch")
      .withRunAsNonRoot(true)
      .withSeccompProfile(SeccompProfileBuilder().withType(SECCOMP_PROFILE_TYPE).build())
      .build()

  fun rootSecurityContext(): PodSecurityContext = PodSecurityContextBuilder().withRunAsUser(ROOT_USER_ID).build()

  private fun baseContainerSecurityContext(
    user: Long,
    group: Long,
  ): SecurityContextBuilder =
    SecurityContextBuilder()
      .withRunAsUser(user)
      .withRunAsGroup(group)
      .withAllowPrivilegeEscalation(false)
      .withReadOnlyRootFilesystem(false)
      .withCapabilities(CapabilitiesBuilder().addAllToDrop(DEFAULT_CAPABILITIES).build())
}
