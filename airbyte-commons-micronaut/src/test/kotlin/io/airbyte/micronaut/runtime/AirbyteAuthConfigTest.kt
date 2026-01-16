/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteAuthConfigDefaultTest {
  @Inject
  private lateinit var airbyteAuthConfig: AirbyteAuthConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_AUTH_DATAPLANE_CLIENT_ID_SECRET_KEY, airbyteAuthConfig.dataplaneCredentials.clientIdSecretKey)
    assertEquals(DEFAULT_AUTH_DATAPLANE_CLIENT_SECRET_SECRET_KEY, airbyteAuthConfig.dataplaneCredentials.clientSecretSecretKey)
    assertEquals(DEFAULT_AUTH_REALM, airbyteAuthConfig.defaultRealm)
    assertEquals(listOf("airbyte-server"), airbyteAuthConfig.identityProvider.audiences)
    assertEquals(emptyList<String>(), airbyteAuthConfig.identityProvider.issuers)
    assertEquals("", airbyteAuthConfig.identityProvider.oidc.appName)
    assertEquals("", airbyteAuthConfig.identityProvider.oidc.audience)
    assertEquals("", airbyteAuthConfig.identityProvider.oidc.clientId)
    assertEquals("", airbyteAuthConfig.identityProvider.oidc.clientSecret)
    assertEquals("", airbyteAuthConfig.identityProvider.oidc.displayName)
    assertEquals("", airbyteAuthConfig.identityProvider.oidc.domain)
    assertEquals("", airbyteAuthConfig.identityProvider.oidc.endpoints.authorizationServerEndpoint)
    assertEquals("", airbyteAuthConfig.identityProvider.oidc.extraScopes)
    assertEquals("email", airbyteAuthConfig.identityProvider.oidc.fields.email)
    assertEquals("iss", airbyteAuthConfig.identityProvider.oidc.fields.issuer)
    assertEquals("name", airbyteAuthConfig.identityProvider.oidc.fields.name)
    assertEquals("sub", airbyteAuthConfig.identityProvider.oidc.fields.sub)
    assertEquals("", airbyteAuthConfig.instanceAdmin.clientId)
    assertEquals("", airbyteAuthConfig.instanceAdmin.clientSecret)
    assertEquals("", airbyteAuthConfig.instanceAdmin.password)
    assertEquals(DEFAULT_AUTH_IDENTITY_PROVIDER_TYPE, airbyteAuthConfig.identityProvider.type)
    assertEquals(false, airbyteAuthConfig.identityProvider.verifyAudience)
    assertEquals(false, airbyteAuthConfig.identityProvider.verifyIssuer)
    assertEquals("", airbyteAuthConfig.initialUser.email)
    assertEquals("", airbyteAuthConfig.initialUser.firstName)
    assertEquals("", airbyteAuthConfig.initialUser.lastName)
    assertEquals("", airbyteAuthConfig.initialUser.password)
    assertEquals(true, airbyteAuthConfig.kubernetesSecret.creationEnabled)
    assertEquals(
      DEFAULT_AUTH_INSTANCE_ADMIN_PASSWORD_SECRET_KEY,
      airbyteAuthConfig.kubernetesSecret.keys.instanceAdminPasswordSecretKey,
    )
    assertEquals(
      DEFAULT_AUTH_INSTANCE_ADMIN_CLIENT_ID_SECRET_KEY,
      airbyteAuthConfig.kubernetesSecret.keys.instanceAdminClientIdSecretKey,
    )
    assertEquals(
      DEFAULT_AUTH_INSTANCE_ADMIN_CLIENT_SECRET_SECRET_KEY,
      airbyteAuthConfig.kubernetesSecret.keys.instanceAdminClientSecretSecretKey,
    )
    assertEquals(DEFAULT_AUTH_JWT_SIGNATURE_SECRET_KEY, airbyteAuthConfig.kubernetesSecret.keys.jwtSignatureSecretKey)
    assertEquals(DEFAULT_AUTH_KUBERNETES_SECRET_NAME, airbyteAuthConfig.kubernetesSecret.name)
    assertEquals("", airbyteAuthConfig.kubernetesSecret.values.instanceAdminPassword)
    assertEquals("", airbyteAuthConfig.kubernetesSecret.values.instanceAdminClientId)
    assertEquals("", airbyteAuthConfig.kubernetesSecret.values.instanceAdminClientSecret)
    assertEquals("", airbyteAuthConfig.kubernetesSecret.values.jwtSignatureSecret)
    assertEquals(DEFAULT_AUTH_APP_TOKEN_EXPIRATION_MINS, airbyteAuthConfig.tokenExpiration.applicationTokenExpirationInMinutes)
    assertEquals(DEFAULT_AUTH_DATAPLANE_TOKEN_EXPIRATION_MINS, airbyteAuthConfig.tokenExpiration.dataplaneTokenExpirationInMinutes)
    assertEquals(DEFAULT_AUTH_EMBEDDED_TOKEN_EXPIRATION_MINS, airbyteAuthConfig.tokenExpiration.embeddedTokenExpirationInMinutes)
    assertEquals(DEFAULT_AUTH_SERVICE_ACCOUNT_TOKEN_EXPIRATION_MINS, airbyteAuthConfig.tokenExpiration.serviceAccountTokenExpirationInMinutes)
    assertEquals(DEFAULT_AUTH_TOKEN_ISSUER, airbyteAuthConfig.tokenIssuer)
  }
}

@MicronautTest(propertySources = ["classpath:application-auth.yml"], environments = [Environment.TEST])
internal class AirbyteBootloaderAuthConfigOverridesTest {
  @Inject
  private lateinit var airbyteAuthConfig: AirbyteAuthConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-dataplane-client-id-secret-key", airbyteAuthConfig.dataplaneCredentials.clientIdSecretKey)
    assertEquals("test-dataplane-client-secret-secret-key", airbyteAuthConfig.dataplaneCredentials.clientSecretSecretKey)
    assertEquals("test-realm", airbyteAuthConfig.defaultRealm)
    assertEquals(listOf("test-audience-1", "test-audience-2"), airbyteAuthConfig.identityProvider.audiences)
    assertEquals(listOf("test-issuer-1", "test-issuer-2"), airbyteAuthConfig.identityProvider.issuers)
    assertEquals("test-oidc-app-name", airbyteAuthConfig.identityProvider.oidc.appName)
    assertEquals("test-oidc-audience", airbyteAuthConfig.identityProvider.oidc.audience)
    assertEquals("test-oidc-client-id", airbyteAuthConfig.identityProvider.oidc.clientId)
    assertEquals("test-oidc-client-secret", airbyteAuthConfig.identityProvider.oidc.clientSecret)
    assertEquals("test-oidc-display-name", airbyteAuthConfig.identityProvider.oidc.displayName)
    assertEquals("test-oidc-domain", airbyteAuthConfig.identityProvider.oidc.domain)
    assertEquals("test-oidc-authorization-server-endpoint", airbyteAuthConfig.identityProvider.oidc.endpoints.authorizationServerEndpoint)
    assertEquals("test-oidc-extra-scopes", airbyteAuthConfig.identityProvider.oidc.extraScopes)
    assertEquals("test-oidc-email", airbyteAuthConfig.identityProvider.oidc.fields.email)
    assertEquals("test-oidc-issuer", airbyteAuthConfig.identityProvider.oidc.fields.issuer)
    assertEquals("test-oidc-name", airbyteAuthConfig.identityProvider.oidc.fields.name)
    assertEquals("test-oidc-sub", airbyteAuthConfig.identityProvider.oidc.fields.sub)
    assertEquals("oidc", airbyteAuthConfig.identityProvider.type)
    assertEquals(true, airbyteAuthConfig.identityProvider.verifyAudience)
    assertEquals(true, airbyteAuthConfig.identityProvider.verifyIssuer)
    assertEquals("test-instance-admin-client-id", airbyteAuthConfig.instanceAdmin.clientId)
    assertEquals("test-instance-admin-client-secret", airbyteAuthConfig.instanceAdmin.clientSecret)
    assertEquals("test-instance-admin-password", airbyteAuthConfig.instanceAdmin.password)
    assertEquals("test@airbyte.io", airbyteAuthConfig.initialUser.email)
    assertEquals("test-first-name", airbyteAuthConfig.initialUser.firstName)
    assertEquals("test-last-name", airbyteAuthConfig.initialUser.lastName)
    assertEquals("test-password", airbyteAuthConfig.initialUser.password)
    assertEquals(false, airbyteAuthConfig.kubernetesSecret.creationEnabled)
    assertEquals("test-instance-admin-password-secret-key", airbyteAuthConfig.kubernetesSecret.keys.instanceAdminPasswordSecretKey)
    assertEquals("test-instance-admin-client-id-secret-key", airbyteAuthConfig.kubernetesSecret.keys.instanceAdminClientIdSecretKey)
    assertEquals(
      "test-instance-admin-client-secret-secret-key",
      airbyteAuthConfig.kubernetesSecret.keys.instanceAdminClientSecretSecretKey,
    )
    assertEquals("test-jwt-signature-secret-key", airbyteAuthConfig.kubernetesSecret.keys.jwtSignatureSecretKey)
    assertEquals("test-name", airbyteAuthConfig.kubernetesSecret.name)
    assertEquals("test-instance-admin-password", airbyteAuthConfig.kubernetesSecret.values.instanceAdminPassword)
    assertEquals("test-instance-admin-client-id", airbyteAuthConfig.kubernetesSecret.values.instanceAdminClientId)
    assertEquals("test-instance-admin-client-secret", airbyteAuthConfig.kubernetesSecret.values.instanceAdminClientSecret)
    assertEquals("test-jwt-signature-secret", airbyteAuthConfig.kubernetesSecret.values.jwtSignatureSecret)
    assertEquals(30, airbyteAuthConfig.tokenExpiration.applicationTokenExpirationInMinutes)
    assertEquals(10, airbyteAuthConfig.tokenExpiration.dataplaneTokenExpirationInMinutes)
    assertEquals(5, airbyteAuthConfig.tokenExpiration.embeddedTokenExpirationInMinutes)
    assertEquals(45, airbyteAuthConfig.tokenExpiration.serviceAccountTokenExpirationInMinutes)
    assertEquals("http://test-token-issuer", airbyteAuthConfig.tokenIssuer)
  }
}
