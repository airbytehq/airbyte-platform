/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteSecretsManagerConfigTest {
  @Inject
  private lateinit var secretsConfig: AirbyteSecretsManagerConfig

  @Test
  fun testLoadingDefaultValuesFromConfig() {
    assertEquals(SecretPersistenceType.NO_OP, secretsConfig.persistence)
    assertInstanceOf(NoOpSecretsManagerConfig::class.java, secretsConfig.getSecretsConfig())
  }
}

@MicronautTest(propertySources = ["classpath:application-secret-aws.yml"])
internal class AirbyteSecretsManagerConfigAwsTest {
  @Inject
  private lateinit var secretsConfig: AirbyteSecretsManagerConfig

  @Test
  fun testLoadingAwsSecretsManagerValuesFromConfig() {
    assertEquals(SecretPersistenceType.AWS_SECRET_MANAGER, secretsConfig.persistence)
    val awsConfig =
      secretsConfig.getSecretsConfig() as
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.AwsSecretsManagerConfig
    assertInstanceOf(
      AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.AwsSecretsManagerConfig::class.java,
      awsConfig,
    )
    assertEquals("test-access-key", awsConfig.accessKey)
    assertEquals("test-secret-key", awsConfig.secretKey)
    assertEquals("us-east-1", awsConfig.region)
    assertEquals("test-access-key-ref-name", awsConfig.accessKeyRefName)
    assertEquals("test-access-key-ref-key", awsConfig.accessKeyRefKey)
    assertEquals("test-secret-key-ref-name", awsConfig.secretKeyRefName)
    assertEquals("test-secret-key-ref-key", awsConfig.secretKeyRefKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-secret-azure.yml"])
internal class AirbyteSecretsManagerConfigAzureTest {
  @Inject
  private lateinit var secretsConfig: AirbyteSecretsManagerConfig

  @Test
  fun testLoadingAzureKeyVaultValuesFromConfig() {
    assertEquals(SecretPersistenceType.AZURE_KEY_VAULT, secretsConfig.persistence)
    val azureConfig =
      secretsConfig.getSecretsConfig() as
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.AzureKeyVaultSecretsManagerConfig
    assertInstanceOf(
      AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.AzureKeyVaultSecretsManagerConfig::class.java,
      azureConfig,
    )
    assertEquals("https://test-vault-url:9999", azureConfig.vaultUrl)
    assertEquals("test-tenant-id", azureConfig.tenantId)
    assertEquals("test-client-id", azureConfig.clientId)
    assertEquals("test-client-secret", azureConfig.clientSecret)
    assertEquals("test-client-id-ref-name", azureConfig.clientIdRefName)
    assertEquals("test-client-id-ref-key", azureConfig.clientIdRefKey)
    assertEquals("test-client-secret-ref-name", azureConfig.clientSecretRefName)
    assertEquals("test-client-secret-ref-key", azureConfig.clientSecretRefKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-secret-gcp.yml"])
internal class AirbyteSecretsManagerConfigGcpTest {
  @Inject
  private lateinit var secretsConfig: AirbyteSecretsManagerConfig

  @Test
  fun testLoadingGoogleSecretsManagerValuesFromConfig() {
    assertEquals(SecretPersistenceType.GOOGLE_SECRET_MANAGER, secretsConfig.persistence)
    val gsmConfig =
      secretsConfig.getSecretsConfig() as
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig
    assertInstanceOf(
      AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig::class.java,
      gsmConfig,
    )
    assertEquals("test-project-id", gsmConfig.projectId)
    assertEquals("test-credentials", gsmConfig.credentials)
    assertEquals("us-central1", gsmConfig.region)
    assertEquals("test-credentials-ref-name", gsmConfig.credentialsRefName)
    assertEquals("test-credentials-ref-key", gsmConfig.credentialsRefKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-secret-vault.yml"])
internal class AirbyteSecretsManagerConfigVaultTest {
  @Inject
  private lateinit var secretsConfig: AirbyteSecretsManagerConfig

  @Test
  fun testLoadingVaultSecretsManagerValuesFromConfig() {
    assertEquals(SecretPersistenceType.VAULT, secretsConfig.persistence)
    val vaultConfig =
      secretsConfig.getSecretsConfig() as
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.VaultSecretsManagerConfig
    assertInstanceOf(
      AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.VaultSecretsManagerConfig::class.java,
      vaultConfig,
    )
    assertEquals(SecretPersistenceType.VAULT, secretsConfig.persistence)
    assertEquals("https://vault:6000", vaultConfig.address)
    assertEquals("/test-secrets", vaultConfig.prefix)
    assertEquals("opensesame", vaultConfig.token)
    assertEquals("test-token-ref-name", vaultConfig.tokenRefName)
    assertEquals("test-token-ref-key", vaultConfig.tokenRefKey)
  }
}
