/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.bettercloud.vault.VaultException
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.VaultSecretsManagerConfig
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.vault.VaultContainer

private const val VAULT_TOKEN = "vault-dev-token-id"
private const val VAULT_CERT_DIR = "/vault/file"

// A self-signed CA that has nothing to do with the Vault under test; its key was thrown away.
private val UNRELATED_CA =
  """
  -----BEGIN CERTIFICATE-----
  MIIBfjCCASSgAwIBAgIUaQ7MhMeQQVH8ZwISES9mYZuy6EUwCgYIKoZIzj0EAwIw
  HDEaMBgGA1UEAwwRVW5yZWxhdGVkIHRlc3QgQ0EwIBcNMjYxMTIyMTkxMDEyWhgP
  MjEyNjEwMjkxOTEwMTJaMBwxGjAYBgNVBAMMEVVucmVsYXRlZCB0ZXN0IENBMFkw
  EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEWJm2Tvt8u0jtH6nZe4aH4qFHuvR1eMOy
  kmqe0wnby9QOnvQHlvhzO+YSRJbjnvCD+Q18CL8ZMFh0nUnS6+LP56NCMEAwHQYD
  VR0OBBYEFN4Anr0KqGGzE2LZX/Bm0mroDPpfMA8GA1UdEwEB/wQFMAMBAf8wDgYD
  VR0PAQH/BAQDAgIEMAoGCCqGSM49BAMCA0gAMEUCIQCIH4Gchb7qpt2mRv8z98Wt
  +ETEmgEtAA7ZFJDkH3R+vwIgW6FCXsvmDpPV8QXq/c558l4cCW8MBedH/Qv7/Zw9
  eJ8=
  -----END CERTIFICATE-----
  """.trimIndent()

/**
 * Runs Vault with a TLS listener whose certificate comes from a CA the JVM does not trust (Vault's dev-tls mode), the
 * way a self-hosted Vault behind a private CA looks to Airbyte.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class VaultSecretPersistenceTlsTest {
  private lateinit var vaultContainer: VaultContainer<*>
  private lateinit var vaultAddress: String
  private lateinit var vaultCa: String

  @BeforeAll
  fun setUp() {
    // The dev certificate covers localhost; name the Docker host too, for when tests reach Docker elsewhere.
    val dockerHost = DockerClientFactory.instance().dockerHostIpAddress()
    vaultContainer =
      VaultContainer("hashicorp/vault:1.21.4")
        .withVaultToken(VAULT_TOKEN)
        .withCommand("server", "-dev", "-dev-tls", "-dev-tls-cert-dir=$VAULT_CERT_DIR", "-dev-tls-san=$dockerHost")
        .waitingFor(Wait.forHttps("/v1/sys/health").allowInsecure().forStatusCode(200))
    vaultContainer.start()
    vaultAddress = "https://${vaultContainer.host}:${vaultContainer.firstMappedPort}"
    vaultCa = vaultContainer.copyFileFromContainer("$VAULT_CERT_DIR/vault-ca.pem") { it.readAllBytes().decodeToString() }
  }

  @AfterAll
  fun tearDown() {
    vaultContainer.stop()
  }

  private fun persistence(caCert: String) =
    VaultSecretPersistence(
      SystemVaultClient(
        VaultSecretsManagerConfig(
          address = vaultAddress,
          prefix = "secret/testing",
          token = VAULT_TOKEN,
          caCert = caCert,
        ),
      ),
    )

  @Test
  fun `reads and writes when given the Vault CA`() {
    val persistence = persistence(caCert = vaultCa)
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_VaultSecretPersistenceTlsTest_ca", 1)

    persistence.write(coordinate, "abc")

    assertEquals("abc", persistence.read(coordinate))
  }

  @Test
  fun `trusts every certificate in a CA bundle, not only the first`() {
    val persistence = persistence(caCert = UNRELATED_CA + "\n" + vaultCa)
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_VaultSecretPersistenceTlsTest_bundle", 1)

    persistence.write(coordinate, "def")

    assertEquals("def", persistence.read(coordinate))
  }

  @Test
  fun `rejects the Vault certificate without its CA`() {
    val client = SystemVaultClient(VaultSecretsManagerConfig(address = vaultAddress, prefix = "secret/testing", token = VAULT_TOKEN))

    assertThrows<VaultException> { client.getClient().logical().read("secret/testing/anything") }
  }

  @Test
  fun `rejects the Vault certificate when the CA setting names only other CAs`() {
    val client =
      SystemVaultClient(
        VaultSecretsManagerConfig(address = vaultAddress, prefix = "secret/testing", token = VAULT_TOKEN, caCert = UNRELATED_CA),
      )

    assertThrows<VaultException> { client.getClient().logical().read("secret/testing/anything") }
  }
}
