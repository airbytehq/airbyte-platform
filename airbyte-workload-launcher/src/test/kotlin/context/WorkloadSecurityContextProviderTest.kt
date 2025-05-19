/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.context

import io.fabric8.kubernetes.api.model.SeccompProfileBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

internal class WorkloadSecurityContextProviderTest {
  @Test
  internal fun testDefaultContainerSecurityContext() {
    val userId = 12345L
    val groupId = 67890L
    val context = WorkloadSecurityContextProvider(rootlessWorkload = true)

    val securityContext = context.defaultContainerSecurityContext(user = userId, group = groupId)
    assertNotNull(securityContext)
    assertEquals(userId, securityContext?.runAsUser)
    assertEquals(groupId, securityContext?.runAsGroup)
    assertEquals(false, securityContext?.allowPrivilegeEscalation)
    assertEquals(false, securityContext?.readOnlyRootFilesystem)
    assertEquals(DEFAULT_CAPABILITIES, securityContext?.capabilities?.drop)
  }

  @Test
  internal fun testDefaultContainerSecurityContextDisabled() {
    val userId = 12345L
    val groupId = 67890L
    val context = WorkloadSecurityContextProvider(rootlessWorkload = false)

    val securityContext = context.defaultContainerSecurityContext(user = userId, group = groupId)
    assertNull(securityContext)
  }

  @Test
  internal fun testRootlessContainerSecurityContext() {
    val secComp = SeccompProfileBuilder().withType(SECCOMP_PROFILE_TYPE).build()
    val context = WorkloadSecurityContextProvider(rootlessWorkload = true)

    val securityContext = context.rootlessContainerSecurityContext()
    assertNotNull(securityContext)
    assertEquals(ROOTLESS_USER_ID, securityContext?.runAsUser)
    assertEquals(ROOTLESS_GROUP_ID, securityContext?.runAsGroup)
    assertEquals(false, securityContext?.allowPrivilegeEscalation)
    assertEquals(false, securityContext?.readOnlyRootFilesystem)
    assertEquals(true, securityContext?.runAsNonRoot)
    assertEquals(secComp, securityContext?.seccompProfile)
    assertEquals(DEFAULT_CAPABILITIES, securityContext?.capabilities?.drop)
  }

  @Test
  internal fun testRootlessContainerSecurityContextDisabled() {
    val context = WorkloadSecurityContextProvider(rootlessWorkload = false)
    val securityContext = context.rootlessContainerSecurityContext()
    assertNull(securityContext)
  }

  @Test
  internal fun testRootlessPodSecurityContext() {
    val userId = 12345L
    val groupId = 67890L
    val secComp = SeccompProfileBuilder().withType(SECCOMP_PROFILE_TYPE).build()
    val context = WorkloadSecurityContextProvider(rootlessWorkload = true)

    val securityContext = context.rootlessPodSecurityContext(user = userId, group = groupId)
    assertNotNull(securityContext)
    assertEquals(userId, securityContext?.runAsUser)
    assertEquals(groupId, securityContext?.runAsGroup)
    assertEquals(groupId, securityContext?.fsGroup)
    assertEquals(true, securityContext?.runAsNonRoot)
    assertEquals(secComp, securityContext?.seccompProfile)
  }

  @Test
  internal fun testRootlessPodSecurityContextDisabled() {
    val userId = 12345L
    val groupId = 67890L
    val context = WorkloadSecurityContextProvider(rootlessWorkload = false)
    val securityContext = context.rootlessPodSecurityContext(user = userId, group = groupId)
    assertNull(securityContext)
  }

  @Test
  internal fun testDefaultPodSecurityContext() {
    val context = WorkloadSecurityContextProvider(rootlessWorkload = true)

    val securityContext = context.defaultPodSecurityContext()
    assertNotNull(securityContext)
    assertEquals(ROOTLESS_GROUP_ID, securityContext?.fsGroup)
  }

  @Test
  internal fun testDefaultPodSecurityContextDisabled() {
    val context = WorkloadSecurityContextProvider(rootlessWorkload = false)
    val securityContext = context.defaultPodSecurityContext()
    assertNull(securityContext)
  }
}
