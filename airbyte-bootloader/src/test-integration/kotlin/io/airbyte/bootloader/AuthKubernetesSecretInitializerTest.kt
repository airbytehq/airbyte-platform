/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.bootloader.K8sSecretHelper.base64Encode
import io.fabric8.kubernetes.client.KubernetesClient
import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.unmockkObject
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal const val SECRET_NAME = "test-secret"
internal const val PASSWORD_KEY = "password"
internal const val CLIENT_ID_KEY = "clientId"
internal const val CLIENT_SECRET_KEY = "clientSecret"
internal const val JWT_SIGNATURE_KEY = "jwtSignature"
internal const val PROVIDED_PASSWORD_VALUE = "hunter2"
internal const val PROVIDED_CLIENT_ID_VALUE = "myClientId"
internal const val PROVIDED_CLIENT_SECRET_VALUE = "myClientSecret"
internal const val PROVIDED_JWT_SIGNATURE_VALUE = "myJwtSignature"

@MicronautTest(environments = [Environment.TEST])
@Property(name = "airbyte.auth.kubernetes-secret.name", value = SECRET_NAME)
class AuthKubernetesSecretInitializerTest {
  private val k8sClient = mockk<KubernetesClient>()
  private val secretKeysConfig = mockk<AuthKubernetesSecretKeysConfig>()
  private val providedSecretValuesConfig = mockk<AuthKubernetesSecretValuesConfig>()

  lateinit var authKubernetesSecretInitializer: AuthKubernetesSecretInitializer

  @BeforeEach
  fun setUp() {
    authKubernetesSecretInitializer =
      AuthKubernetesSecretInitializer(
        SECRET_NAME,
        k8sClient,
        secretKeysConfig,
        providedSecretValuesConfig,
      )
    mockkObject(K8sSecretHelper)
    every { K8sSecretHelper.createOrUpdateSecret(any(), any(), any()) } returns Unit
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
    unmockkObject(K8sSecretHelper)
  }

  @Test
  fun `test initializeSecrets when secret does not exist`() {
    setupSecretKeysConfig()
    every { k8sClient.secrets().withName(SECRET_NAME).get() } returns null
    every { providedSecretValuesConfig.instanceAdminPassword } returns PROVIDED_PASSWORD_VALUE
    every { providedSecretValuesConfig.instanceAdminClientId } returns PROVIDED_CLIENT_ID_VALUE
    every { providedSecretValuesConfig.instanceAdminClientSecret } returns PROVIDED_CLIENT_SECRET_VALUE
    every { providedSecretValuesConfig.jwtSignatureSecret } returns PROVIDED_JWT_SIGNATURE_VALUE

    authKubernetesSecretInitializer.initializeSecrets()

    verify {
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          PASSWORD_KEY to PROVIDED_PASSWORD_VALUE,
          CLIENT_ID_KEY to PROVIDED_CLIENT_ID_VALUE,
          CLIENT_SECRET_KEY to PROVIDED_CLIENT_SECRET_VALUE,
          JWT_SIGNATURE_KEY to PROVIDED_JWT_SIGNATURE_VALUE,
        ),
      )
    }
  }

  @Test
  fun `test initializeSecrets when secret already exists`() {
    setupSecretKeysConfig()
    every { k8sClient.secrets().withName(SECRET_NAME).get() } returns mockk()
    every { providedSecretValuesConfig.instanceAdminPassword } returns PROVIDED_PASSWORD_VALUE
    every { providedSecretValuesConfig.instanceAdminClientId } returns PROVIDED_CLIENT_ID_VALUE
    every { providedSecretValuesConfig.instanceAdminClientSecret } returns PROVIDED_CLIENT_SECRET_VALUE
    every { providedSecretValuesConfig.jwtSignatureSecret } returns PROVIDED_JWT_SIGNATURE_VALUE

    authKubernetesSecretInitializer.initializeSecrets()

    verify {
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          PASSWORD_KEY to PROVIDED_PASSWORD_VALUE,
          CLIENT_ID_KEY to PROVIDED_CLIENT_ID_VALUE,
          CLIENT_SECRET_KEY to PROVIDED_CLIENT_SECRET_VALUE,
          JWT_SIGNATURE_KEY to PROVIDED_JWT_SIGNATURE_VALUE,
        ),
      )
    }
  }

  @Test
  fun `test initializeSecrets with mix of existing, provided, and pre-existing values`() {
    setupSecretKeysConfig()
    val preExistingClientId = "preExistingClientId"
    val preExistingClientSecret = "preExistingClientSecret"

    every { k8sClient.secrets().withName(SECRET_NAME).get() } returns
      mockk {
        every { data } returns
          mapOf(
            CLIENT_ID_KEY to base64Encode(preExistingClientId),
            CLIENT_SECRET_KEY to base64Encode(preExistingClientSecret),
          )
      }

    every { providedSecretValuesConfig.instanceAdminPassword } returns null
    every { providedSecretValuesConfig.instanceAdminClientId } returns null
    every { providedSecretValuesConfig.instanceAdminClientSecret } returns PROVIDED_CLIENT_SECRET_VALUE
    every { providedSecretValuesConfig.jwtSignatureSecret } returns PROVIDED_JWT_SIGNATURE_VALUE

    authKubernetesSecretInitializer.initializeSecrets()

    verify {
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        match<Map<String, String>> {
          it.size == 4 &&
            // not provided, generated random
            it[PASSWORD_KEY]?.length == SECRET_LENGTH &&
            // not provided, left alone
            it[CLIENT_ID_KEY] == preExistingClientId &&
            // provided, replaced existing
            it[CLIENT_SECRET_KEY] == PROVIDED_CLIENT_SECRET_VALUE &&
            // provided, no existing so simply added
            it[JWT_SIGNATURE_KEY] == PROVIDED_JWT_SIGNATURE_VALUE
        },
      )
    }
  }

  private fun setupSecretKeysConfig() {
    every { secretKeysConfig.instanceAdminPasswordSecretKey } returns PASSWORD_KEY
    every { secretKeysConfig.instanceAdminClientIdSecretKey } returns CLIENT_ID_KEY
    every { secretKeysConfig.instanceAdminClientSecretSecretKey } returns CLIENT_SECRET_KEY
    every { secretKeysConfig.jwtSignatureSecretKey } returns JWT_SIGNATURE_KEY
  }
}
