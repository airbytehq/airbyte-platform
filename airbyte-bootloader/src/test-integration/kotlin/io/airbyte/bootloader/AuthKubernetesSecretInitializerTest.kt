package io.airbyte.bootloader

import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.api.model.SecretBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.NamespaceableResource
import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.MockKAnnotations
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.slot
import io.mockk.unmockkAll
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Base64

private const val SECRET_NAME = "test-secret"
private const val PASSWORD_KEY = "password"
private const val CLIENT_ID_KEY = "clientId"
private const val CLIENT_SECRET_KEY = "clientSecret"
private const val JWT_SIGNATURE_KEY = "jwtSignature"
private const val PROVIDED_PASSWORD_VALUE = "hunter2"
private const val PROVIDED_CLIENT_ID_VALUE = "myClientId"
private const val PROVIDED_CLIENT_SECRET_VALUE = "myClientSecret"
private const val PROVIDED_JWT_SIGNATURE_VALUE = "myJwtSignature"

@MicronautTest(environments = [Environment.TEST])
@Property(name = "airbyte.auth.kubernetes-secret.creation-enabled", value = "true")
@Property(name = "airbyte.auth.kubernetes-secret.name", value = SECRET_NAME)
class AuthKubernetesSecretInitializerTest {
  @MockK
  lateinit var mockKubernetesClient: KubernetesClient

  @MockK
  lateinit var mockSecretKeysConfig: AuthKubernetesSecretKeysConfig

  @MockK
  lateinit var mockProvidedSecretValuesConfig: AuthKubernetesSecretValuesConfig

  @MockK
  lateinit var mockResource: NamespaceableResource<Secret>

  @MockK
  lateinit var mockSecret: Secret

  lateinit var authKubernetesSecretInitializer: AuthKubernetesSecretInitializer

  @BeforeEach
  fun setUp() {
    MockKAnnotations.init(this)
    authKubernetesSecretInitializer =
      AuthKubernetesSecretInitializer(
        SECRET_NAME,
        mockKubernetesClient,
        mockSecretKeysConfig,
        mockProvidedSecretValuesConfig,
      )
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun `test initializeSecrets when secret does not exist`() {
    every { mockKubernetesClient.secrets().withName(any()).get() } returns null
    every { mockKubernetesClient.resource(any<Secret>()) } returns mockResource
    every { mockResource.create() } returns mockSecret
    setupSecretKeysConfig()
    setupProvidedSecretValuesConfig()

    authKubernetesSecretInitializer.initializeSecrets()

    val secretSlot = slot<Secret>()
    verify { mockResource.create() }
    verify { mockKubernetesClient.resource(capture(secretSlot)) }
    val capturedSecret = secretSlot.captured
    assertEquals(SECRET_NAME, capturedSecret.metadata.name)
    assertEquals(4, capturedSecret.data.size)
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_PASSWORD_VALUE.toByteArray()), capturedSecret.data[PASSWORD_KEY])
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_CLIENT_ID_VALUE.toByteArray()), capturedSecret.data[CLIENT_ID_KEY])
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_CLIENT_SECRET_VALUE.toByteArray()), capturedSecret.data[CLIENT_SECRET_KEY])
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_JWT_SIGNATURE_VALUE.toByteArray()), capturedSecret.data[JWT_SIGNATURE_KEY])
  }

  @Test
  fun `test initializeSecrets when secret already exists`() {
    val existingSecret =
      SecretBuilder()
        .withNewMetadata()
        .withName(SECRET_NAME)
        .endMetadata()
        .build()
    every { mockKubernetesClient.secrets().withName(any()).get() } returns existingSecret
    every { mockKubernetesClient.resource(any<Secret>()) } returns mockResource
    every { mockResource.update() } returns mockSecret
    setupSecretKeysConfig()
    setupProvidedSecretValuesConfig()

    authKubernetesSecretInitializer.initializeSecrets()

    val secretSlot = slot<Secret>()
    verify { mockResource.update() }
    verify { mockKubernetesClient.resource(capture(secretSlot)) }
    val capturedSecret = secretSlot.captured
    assertEquals(SECRET_NAME, capturedSecret.metadata.name)
    assertEquals(4, capturedSecret.data.size)
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_PASSWORD_VALUE.toByteArray()), capturedSecret.data[PASSWORD_KEY])
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_CLIENT_ID_VALUE.toByteArray()), capturedSecret.data[CLIENT_ID_KEY])
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_CLIENT_SECRET_VALUE.toByteArray()), capturedSecret.data[CLIENT_SECRET_KEY])
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_JWT_SIGNATURE_VALUE.toByteArray()), capturedSecret.data[JWT_SIGNATURE_KEY])
  }

  @Test
  fun `test initializeSecrets when some values are not provided`() {
    val existingSecret =
      SecretBuilder()
        .withNewMetadata()
        .withName(SECRET_NAME)
        .endMetadata()
        // clientId is already set in the secret, but a new value was provided, so it should be updated.
        .addToData(CLIENT_ID_KEY, Base64.getEncoder().encodeToString("preExistingClientId".toByteArray()))
        // clientSecret is already set in the secret, and no new value was provided, so it should remain the same.
        .addToData(CLIENT_SECRET_KEY, Base64.getEncoder().encodeToString(PROVIDED_CLIENT_SECRET_VALUE.toByteArray()))
        .build()
    every { mockKubernetesClient.secrets().withName(any()).get() } returns existingSecret
    every { mockKubernetesClient.resource(any<Secret>()) } returns mockResource
    every { mockResource.update() } returns mockSecret
    setupSecretKeysConfig()
    setupProvidedSecretValuesConfigWithoutPassword()

    authKubernetesSecretInitializer.initializeSecrets()

    val secretSlot = slot<Secret>()
    verify { mockResource.update() }
    verify { mockKubernetesClient.resource(capture(secretSlot)) }
    val capturedSecret = secretSlot.captured
    assertEquals(SECRET_NAME, capturedSecret.metadata.name)
    assertEquals(4, capturedSecret.data.size)
    // since the password is randomly generated, verify the length of the password matches the expected length
    assertEquals(SECRET_LENGTH, Base64.getDecoder().decode(capturedSecret.data[PASSWORD_KEY]).size)
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_CLIENT_ID_VALUE.toByteArray()), capturedSecret.data[CLIENT_ID_KEY])
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_CLIENT_SECRET_VALUE.toByteArray()), capturedSecret.data[CLIENT_SECRET_KEY])
    assertEquals(Base64.getEncoder().encodeToString(PROVIDED_JWT_SIGNATURE_VALUE.toByteArray()), capturedSecret.data[JWT_SIGNATURE_KEY])
  }

  private fun setupProvidedSecretValuesConfigWithoutPassword() {
    every { mockProvidedSecretValuesConfig.instanceAdminPassword } returns null
    every { mockProvidedSecretValuesConfig.instanceAdminClientId } returns PROVIDED_CLIENT_ID_VALUE
    every { mockProvidedSecretValuesConfig.instanceAdminClientSecret } returns PROVIDED_CLIENT_SECRET_VALUE
    every { mockProvidedSecretValuesConfig.jwtSignatureSecret } returns PROVIDED_JWT_SIGNATURE_VALUE
  }

  private fun setupSecretKeysConfig() {
    every { mockSecretKeysConfig.instanceAdminPasswordSecretKey } returns PASSWORD_KEY
    every { mockSecretKeysConfig.instanceAdminClientIdSecretKey } returns CLIENT_ID_KEY
    every { mockSecretKeysConfig.instanceAdminClientSecretSecretKey } returns CLIENT_SECRET_KEY
    every { mockSecretKeysConfig.jwtSignatureSecretKey } returns JWT_SIGNATURE_KEY
  }

  private fun setupProvidedSecretValuesConfig() {
    every { mockProvidedSecretValuesConfig.instanceAdminPassword } returns PROVIDED_PASSWORD_VALUE
    every { mockProvidedSecretValuesConfig.instanceAdminClientId } returns PROVIDED_CLIENT_ID_VALUE
    every { mockProvidedSecretValuesConfig.instanceAdminClientSecret } returns PROVIDED_CLIENT_SECRET_VALUE
    every { mockProvidedSecretValuesConfig.jwtSignatureSecret } returns PROVIDED_JWT_SIGNATURE_VALUE
  }
}
