/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package config

import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig
import io.airbyte.micronaut.runtime.SecretPersistenceType
import io.airbyte.workers.pod.Metadata.AWS_ACCESS_KEY_ID
import io.airbyte.workers.pod.Metadata.AWS_SECRET_ACCESS_KEY
import io.airbyte.workload.launcher.config.EnvVarConfigBeanFactory
import io.airbyte.workload.launcher.constants.EnvVarConstants.AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR
import io.airbyte.workload.launcher.constants.EnvVarConstants.AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR
import io.airbyte.workload.launcher.constants.EnvVarConstants.VAULT_ADDRESS
import io.airbyte.workload.launcher.constants.EnvVarConstants.VAULT_CA_CERT
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test

class EnvVarConfigBeanFactoryTest {
  companion object {
    const val AWS_ASSUMED_ROLE_ACCESS_KEY = "accessKey"
    const val AWS_ASSUMED_ROLE_SECRET_KEY = "secretKey"
    const val AWS_ASSUMED_ROLE_SECRET_NAME = "secretName"
  }

  @Test
  fun `orchestrator aws assumed role secret creation`() {
    val factory = EnvVarConfigBeanFactory()
    val airbyteConnectorConfig =
      AirbyteConnectorConfig(
        source =
          AirbyteConnectorConfig.AirbyteSourceConnectorConfig(
            credentials =
              AirbyteConnectorConfig.AirbyteSourceConnectorConfig.AirbyteSourceConnectorCredentialsConfig(
                aws =
                  AirbyteConnectorConfig.AirbyteSourceConnectorConfig.AirbyteSourceConnectorCredentialsConfig
                    .AirbyteSourceConnectorAwsCredentialsConfig(
                      assumedRole =
                        AirbyteConnectorConfig.AirbyteSourceConnectorConfig.AirbyteSourceConnectorCredentialsConfig
                          .AirbyteSourceConnectorAwsCredentialsConfig
                          .AirbyteSourceConnectorAwsAssumedRoleConfig(
                            accessKey = AWS_ASSUMED_ROLE_ACCESS_KEY,
                            secretKey = AWS_ASSUMED_ROLE_SECRET_KEY,
                            secretName = AWS_ASSUMED_ROLE_SECRET_NAME,
                          ),
                    ),
              ),
          ),
      )
    val envMap =
      factory.awsAssumedRoleSecretEnv(airbyteConnectorConfig)

    assertEquals(2, envMap.size)
    val awsAccessKey = envMap[AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR]
    val awsAccessKeySecretRef = awsAccessKey!!.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsAccessKeySecretRef.name)
    assertEquals(AWS_ASSUMED_ROLE_ACCESS_KEY, awsAccessKeySecretRef.key)

    val awsSecretKey = envMap[AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR]
    val awsSecretKeyRef = awsSecretKey!!.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsSecretKeyRef.name)
    assertEquals(AWS_ASSUMED_ROLE_SECRET_KEY, awsSecretKeyRef.key)
  }

  @Test
  fun `orchestrator aws assumed role secret creation with blank names`() {
    val factory = EnvVarConfigBeanFactory()
    val airbyteConnectorConfig =
      AirbyteConnectorConfig(
        source =
          AirbyteConnectorConfig.AirbyteSourceConnectorConfig(
            credentials =
              AirbyteConnectorConfig.AirbyteSourceConnectorConfig.AirbyteSourceConnectorCredentialsConfig(
                aws =
                  AirbyteConnectorConfig.AirbyteSourceConnectorConfig.AirbyteSourceConnectorCredentialsConfig
                    .AirbyteSourceConnectorAwsCredentialsConfig(
                      assumedRole =
                        AirbyteConnectorConfig.AirbyteSourceConnectorConfig.AirbyteSourceConnectorCredentialsConfig
                          .AirbyteSourceConnectorAwsCredentialsConfig
                          .AirbyteSourceConnectorAwsAssumedRoleConfig(
                            accessKey = AWS_ASSUMED_ROLE_ACCESS_KEY,
                            secretKey = AWS_ASSUMED_ROLE_SECRET_KEY,
                          ),
                    ),
              ),
          ),
      )
    val envMap =
      factory.awsAssumedRoleSecretEnv(airbyteConnectorConfig)
    assertEquals(0, envMap.size)
  }

  @Test
  fun `connector aws assumed role secret creation`() {
    val factory = EnvVarConfigBeanFactory()
    val airbyteConnectorConfig =
      AirbyteConnectorConfig(
        source =
          AirbyteConnectorConfig.AirbyteSourceConnectorConfig(
            credentials =
              AirbyteConnectorConfig.AirbyteSourceConnectorConfig.AirbyteSourceConnectorCredentialsConfig(
                aws =
                  AirbyteConnectorConfig.AirbyteSourceConnectorConfig
                    .AirbyteSourceConnectorCredentialsConfig
                    .AirbyteSourceConnectorAwsCredentialsConfig(
                      assumedRole =
                        AirbyteConnectorConfig.AirbyteSourceConnectorConfig.AirbyteSourceConnectorCredentialsConfig
                          .AirbyteSourceConnectorAwsCredentialsConfig
                          .AirbyteSourceConnectorAwsAssumedRoleConfig(
                            accessKey = AWS_ASSUMED_ROLE_ACCESS_KEY,
                            secretKey = AWS_ASSUMED_ROLE_SECRET_KEY,
                            secretName = AWS_ASSUMED_ROLE_SECRET_NAME,
                          ),
                    ),
              ),
          ),
      )
    val envList =
      factory.connectorAwsAssumedRoleSecretEnv(airbyteConnectorConfig)

    val awsAccessKey = envList.find { it.name == AWS_ACCESS_KEY_ID }
    val awsAccessKeySecretRef = awsAccessKey!!.valueFrom.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsAccessKeySecretRef.name)
    assertEquals(AWS_ASSUMED_ROLE_ACCESS_KEY, awsAccessKeySecretRef.key)

    val awsSecretKey = envList.find { it.name == AWS_SECRET_ACCESS_KEY }
    val awsSecretKeyRef = awsSecretKey!!.valueFrom.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsSecretKeyRef.name)
    assertEquals(AWS_ASSUMED_ROLE_SECRET_KEY, awsSecretKeyRef.key)
  }

  @Test
  fun `vault CA certificate is passed to the init container`() {
    val caCert = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
    val envMap = EnvVarConfigBeanFactory().secretPersistenceEnvMap(vaultSecretsManagerConfig(caCert = caCert))

    assertEquals("https://vault.example:8200", envMap[VAULT_ADDRESS])
    assertEquals(caCert, envMap[VAULT_CA_CERT])
  }

  @Test
  fun `vault CA certificate is left out when not configured`() {
    val envMap = EnvVarConfigBeanFactory().secretPersistenceEnvMap(vaultSecretsManagerConfig(caCert = ""))

    assertFalse(envMap.containsKey(VAULT_CA_CERT))
  }

  private fun vaultSecretsManagerConfig(caCert: String) =
    AirbyteSecretsManagerConfig(
      persistence = SecretPersistenceType.VAULT,
      store =
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig(
          vault =
            AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.VaultSecretsManagerConfig(
              address = "https://vault.example:8200",
              prefix = "secret/",
              caCert = caCert,
            ),
        ),
    )
}
