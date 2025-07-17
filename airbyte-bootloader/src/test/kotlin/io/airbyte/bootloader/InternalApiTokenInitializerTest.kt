/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import com.nimbusds.jwt.JWTParser
import io.airbyte.bootloader.K8sSecretHelper.base64Encode
import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.slot
import io.mockk.unmockkObject
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class InternalApiTokenInitializerTest {
  private val secretName = "test-secret-name"
  private val keysConfig =
    AuthKubernetesSecretKeysConfig().apply {
      jwtSignatureSecretKey = "jwt-signature-secret"
      internalApiTokenSecretKey = "internal-api-token"
    }

  @BeforeEach
  fun setup() {
    mockkObject(K8sSecretHelper)
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
    unmockkObject(K8sSecretHelper)
  }

  @Test
  fun testInitialize() {
    val k8sClient =
      mockk<KubernetesClient> {
        every { secrets().withName(secretName).get() } returns
          Secret().apply {
            data["jwt-signature-secret"] = base64Encode("abcdefghijklmnopqrstuvwxyzABCDEFGHIJK")
          }
      }

    val dataSlot = slot<Map<String, String>>()
    every { K8sSecretHelper.createOrUpdateSecret(k8sClient, secretName, capture(dataSlot)) } returns Unit

    InternalApiTokenInitializer(secretName, k8sClient, keysConfig, AuthKubernetesSecretValuesConfig())
      .initializeInternalClientToken()

    val claims = JWTParser.parse(dataSlot.captured["internal-api-token"]).jwtClaimsSet
    assertEquals("airbyte-internal-api-client", claims.subject)
    assertEquals(null, claims.expirationTime)
    assertEquals("io.airbyte.auth.internal_client", claims.getStringClaim("typ"))
  }

  @Test
  fun testInitializeUsingConfiguredJwtValue() {
    val k8sClient =
      mockk<KubernetesClient> {
        every { secrets().withName(secretName).get() } returns Secret()
      }
    val valsConfig =
      AuthKubernetesSecretValuesConfig().apply {
        jwtSignatureSecret = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK"
      }

    val dataSlot = slot<Map<String, String>>()
    every { K8sSecretHelper.createOrUpdateSecret(k8sClient, secretName, capture(dataSlot)) } returns Unit

    InternalApiTokenInitializer(secretName, k8sClient, keysConfig, valsConfig)
      .initializeInternalClientToken()

    val claims = JWTParser.parse(dataSlot.captured["internal-api-token"]).jwtClaimsSet
    assertEquals("airbyte-internal-api-client", claims.subject)
    assertEquals(null, claims.expirationTime)
    assertEquals("io.airbyte.auth.internal_client", claims.getStringClaim("typ"))
  }

  @Test
  fun testDontInitializeExisting() {
    val k8sClient =
      mockk<KubernetesClient> {
        every { secrets().withName(secretName).get() } returns
          Secret().apply {
            data["jwt-signature-secret"] = base64Encode("abcdefghijklmnopqrstuvwxyzABCDEFGHIJK")
            data["internal-api-token"] = base64Encode("existing")
          }
      }
    every { K8sSecretHelper.createOrUpdateSecret(any(), any(), any()) } returns Unit

    InternalApiTokenInitializer(secretName, k8sClient, keysConfig, AuthKubernetesSecretValuesConfig())
      .initializeInternalClientToken()

    verify(exactly = 0) {
      K8sSecretHelper.createOrUpdateSecret(any(), any(), any())
    }
  }
}
