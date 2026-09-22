/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import com.auth0.jwt.algorithms.Algorithm
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigStatus
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.airbyte.featureflag.ConfigurableSsoDefaultRole
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import jakarta.ws.rs.NotFoundException
import jakarta.ws.rs.core.Response
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.ClientResource
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.IdentityProviderResource
import org.keycloak.admin.client.resource.IdentityProvidersResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.RealmsResource
import org.keycloak.representations.idm.ClientRepresentation
import org.keycloak.representations.idm.CredentialRepresentation
import org.keycloak.representations.idm.IdentityProviderRepresentation
import org.keycloak.representations.idm.ProtocolMapperRepresentation
import org.keycloak.representations.idm.RealmRepresentation
import java.util.UUID

class AirbyteKeycloakClientTest {
  private val airbyteUrl: String = "https://cloud.airbyte.com"
  private val airbyteAgentsUrl: String = "https://app.airbyte.ai"
  private val airbyteAgentsValidRedirectUris = listOf("$airbyteAgentsUrl/*", "https://staging-app.airbyte.ai/*")
  private val airbyteAgentsWebOrigins = listOf(airbyteAgentsUrl, "https://staging-app.airbyte.ai")
  private val airbyteConfig: AirbyteConfig =
    AirbyteConfig(
      airbyteUrl = airbyteUrl,
      airbyteAgentsUrl = airbyteAgentsUrl,
      airbyteAgentsValidRedirectUris = airbyteAgentsValidRedirectUris,
      airbyteAgentsWebOrigins = airbyteAgentsWebOrigins,
    )
  private val cloudAirbyteConfig: AirbyteConfig = airbyteConfig.copy(edition = AirbyteEdition.CLOUD)
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfig
  private lateinit var airbyteKeycloakAdminClientProvider: AirbyteKeycloakAdminClientProvider
  private lateinit var airbyteKeycloakClient: AirbyteKeycloakClient
  private lateinit var mockHttpClient: OkHttpClient
  private lateinit var mockMetricClient: MetricClient
  private lateinit var mockFeatureFlagClient: FeatureFlagClient

  private var keycloakClientMock = mockk<Keycloak>(relaxed = true)

  @BeforeEach
  fun setup() {
    airbyteKeycloakAdminClientProvider = mockk<AirbyteKeycloakAdminClientProvider>(relaxed = true)
    keycloakConfiguration = mockk<AirbyteKeycloakConfig>(relaxed = true)
    mockHttpClient = mockk<OkHttpClient>()
    mockMetricClient = mockk<MetricClient>(relaxed = true)
    mockFeatureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
    // ConfigurableSsoDefaultRole defaults to ON in tests so the existing Sonar client assertions run.
    every { mockFeatureFlagClient.boolVariation(ConfigurableSsoDefaultRole, any()) } returns true
    every { airbyteKeycloakAdminClientProvider.createKeycloakAdminClient() } returns keycloakClientMock
    airbyteKeycloakClient =
      AirbyteKeycloakClient(
        airbyteKeycloakAdminClientProvider,
        airbyteConfig,
        keycloakConfiguration,
        mockHttpClient,
        mockMetricClient,
        mockFeatureFlagClient,
      )
  }

  @AfterEach
  fun tearDown() {
    clearMocks(keycloakClientMock, mockMetricClient)
  }

  @Test
  fun `createOidcSsoConfig should create realm successfully`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )

    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    val capturedClientRepresentations = mutableListOf<ClientRepresentation>()
    every { clientsMock.create(capture(capturedClientRepresentations)) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockResponse
    // Mock the importFrom call to return a valid OIDC discovery document
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )

    airbyteKeycloakClient.createOidcSsoConfig(config)

    verify(exactly = 5) { keycloakClientMock.realms() }
    verify(exactly = 1) { realmsMock.create(any()) }
    verify(exactly = 2) { realmMock.identityProviders() }
    verify(exactly = 1) { idpMock.create(any()) }
    verify(exactly = 2) { realmMock.clients() }
    verify(exactly = 2) { clientsMock.create(any()) }

    val createdClients = capturedClientRepresentations.associateBy { it.clientId }
    assertWebappClient(
      createdClients.getValue("airbyte-webapp"),
      "Airbyte Webapp",
      airbyteUrl,
      listOf("$airbyteUrl/*"),
      listOf(airbyteUrl),
    )
    assertWebappClient(
      createdClients.getValue("sonar-webapp"),
      "Sonar Webapp",
      airbyteAgentsUrl,
      airbyteAgentsValidRedirectUris,
      airbyteAgentsWebOrigins,
    )
  }

  @Test
  fun `createOidcSsoConfig sets trustEmail on the created identity provider`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )

    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    every { clientsMock.create(any()) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    val capturedIdp = slot<IdentityProviderRepresentation>()
    every { idpMock.create(capture(capturedIdp)) } returns mockResponse
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )

    airbyteKeycloakClient.createOidcSsoConfig(config)

    assertTrue(capturedIdp.captured.isTrustEmail)
  }

  @Test
  fun `createOidcSsoConfig does not create the sonar webapp client when ConfigurableSsoDefaultRole is off`() {
    every { mockFeatureFlagClient.boolVariation(ConfigurableSsoDefaultRole, any()) } returns false

    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )

    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    val capturedClientRepresentations = mutableListOf<ClientRepresentation>()
    every { clientsMock.create(capture(capturedClientRepresentations)) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockResponse
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )

    airbyteKeycloakClient.createOidcSsoConfig(config)

    // Only the airbyte-webapp client should be created while the flag is off.
    verify(exactly = 1) { clientsMock.create(any()) }
    val createdClients = capturedClientRepresentations.associateBy { it.clientId }
    assertTrue(createdClients.containsKey("airbyte-webapp"))
    assertFalse(createdClients.containsKey("sonar-webapp"))
  }

  @Test
  fun `createOidcSsoConfig trims trailing slash from sonar webapp URL`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )
    val airbyteConfigWithTrailingSlash =
      AirbyteConfig(
        airbyteUrl = airbyteUrl,
        airbyteAgentsUrl = "$airbyteAgentsUrl/",
        airbyteAgentsValidRedirectUris = airbyteAgentsValidRedirectUris,
        airbyteAgentsWebOrigins = airbyteAgentsWebOrigins,
      )
    val airbyteKeycloakClientWithTrailingSlash =
      AirbyteKeycloakClient(
        airbyteKeycloakAdminClientProvider,
        airbyteConfigWithTrailingSlash,
        keycloakConfiguration,
        mockHttpClient,
        mockMetricClient,
        mockFeatureFlagClient,
      )

    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    val capturedClientRepresentations = mutableListOf<ClientRepresentation>()
    every { clientsMock.create(capture(capturedClientRepresentations)) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockResponse
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )

    airbyteKeycloakClientWithTrailingSlash.createOidcSsoConfig(config)

    val createdClients = capturedClientRepresentations.associateBy { it.clientId }
    assertWebappClient(
      createdClients.getValue("sonar-webapp"),
      "Sonar Webapp",
      airbyteAgentsUrl,
      airbyteAgentsValidRedirectUris,
      airbyteAgentsWebOrigins,
    )
  }

  @Test
  fun `createOidcSsoConfig falls back to sonar base URL when redirect URIs and web origins are not configured`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )
    val airbyteConfigWithoutSonarLists =
      AirbyteConfig(
        airbyteUrl = airbyteUrl,
        airbyteAgentsUrl = airbyteAgentsUrl,
      )
    val airbyteKeycloakClientWithoutSonarLists =
      AirbyteKeycloakClient(
        airbyteKeycloakAdminClientProvider,
        airbyteConfigWithoutSonarLists,
        keycloakConfiguration,
        mockHttpClient,
        mockMetricClient,
        mockFeatureFlagClient,
      )

    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    val capturedClientRepresentations = mutableListOf<ClientRepresentation>()
    every { clientsMock.create(capture(capturedClientRepresentations)) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockResponse
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )

    airbyteKeycloakClientWithoutSonarLists.createOidcSsoConfig(config)

    val createdClients = capturedClientRepresentations.associateBy { it.clientId }
    assertWebappClient(
      createdClients.getValue("sonar-webapp"),
      "Sonar Webapp",
      airbyteAgentsUrl,
      listOf("$airbyteAgentsUrl/*"),
      listOf(airbyteAgentsUrl),
    )
  }

  @Test
  fun `createOidcSsoConfig should throw RealmCreationException when Keycloak call fails`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )

    every { keycloakClientMock.realms().create(any()) } throws RuntimeException("Internal Server Error")

    val exception = assertThrows<RealmCreationException> { airbyteKeycloakClient.createOidcSsoConfig(config) }

    assertTrue(exception.message!!.contains("Create SSO config request failed"))
  }

  @Test
  fun `createOidcSsoConfig should throw InvalidOidcDiscoveryDocumentException when required fields are missing`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    // Mock importFrom to return an empty discovery document
    every { idpMock.importFrom(any()) } returns emptyMap()

    val exception = assertThrows<InvalidOidcDiscoveryDocumentException> { airbyteKeycloakClient.createOidcSsoConfig(config) }

    assertTrue(exception.message!!.contains("OIDC discovery document missing required fields"))
    assertTrue(exception.missingFields.containsAll(listOf("authorizationUrl", "tokenUrl")))
  }

  @Test
  fun `createOidcSsoConfig should throw ImportConfigException when importFrom fails`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    // Mock importFrom to throw an exception
    every { idpMock.importFrom(any()) } throws RuntimeException("Discovery endpoint unreachable")

    val exception = assertThrows<ImportConfigException> { airbyteKeycloakClient.createOidcSsoConfig(config) }

    assertTrue(exception.message!!.contains("Import SSO config request failed"))
  }

  @Test
  fun `updateIdpClientCredentials successfully changes the idp`() {
    val config =
      SsoKeycloakIdpCredentials(
        organizationId = UUID.randomUUID(),
        clientId = "client-id",
        clientSecret = "client-secret",
      )

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val idpRepresentationMock = mockk<IdentityProviderRepresentation>(relaxed = true)
    every { idpRepresentationMock.alias } returns "default"

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { idpMock.findAll() } returns listOf(idpRepresentationMock)
    every { realmMock.identityProviders() } returns idpMock

    // note that this is a IdentityProviderResource, not an IdentityProvidersResource
    // as above (singular Provider, not plural Providers)!
    val idpProviderMock = mockk<IdentityProviderResource>(relaxed = true)
    every { idpMock.get(any()) } returns idpProviderMock
    every { idpProviderMock.update(any()) } returns Unit

    airbyteKeycloakClient.updateIdpClientCredentials(config, "testrealm")
    verify(exactly = 1) { keycloakClientMock.realms() }
    verify(exactly = 2) { realmMock.identityProviders() }
    verify(exactly = 1) { idpMock.findAll() }
    verify(exactly = 1) { idpMock.get(any()) }
    verify(exactly = 1) { idpProviderMock.update(any()) }
  }

  @Test
  fun `replaceOidcIdpConfig sets trustEmail when no identity provider exists yet`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        status = SsoConfigStatus.ACTIVE,
      )

    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    // No existing default IDP: replaceOidcIdpConfig falls into its "create new" branch, which is
    // the one that constructs a fresh IdentityProviderRepresentation.
    every { idpMock.findAll() } returns emptyList()
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )
    val capturedIdp = slot<IdentityProviderRepresentation>()
    every { idpMock.create(capture(capturedIdp)) } returns mockResponse

    airbyteKeycloakClient.replaceOidcIdpConfig(config)

    assertTrue(capturedIdp.captured.isTrustEmail)
  }

  @Test
  fun `validateToken succeeds for valid token with valid userinfo response`() {
    val validToken = VALID_ACCESS_TOKEN
    val userInfoEndpoint = "http://localhost/realms/master/protocol/openid-connect/userinfo"

    every { keycloakConfiguration.getKeycloakUserInfoEndpointForRealm(any()) } returns userInfoEndpoint

    val mockCall = mockk<Call>()
    val mockResponse =
      okhttp3.Response
        .Builder()
        .request(Request.Builder().url(userInfoEndpoint).build())
        .protocol(Protocol.HTTP_1_1)
        .code(200)
        .message("OK")
        .body("{\"sub\":\"0f0cbf9a-24c2-46cc-b582-d1ff2c0d5ef5\",\"preferred_username\":\"airbyte\"}".toResponseBody())
        .build()

    every { mockHttpClient.newCall(any()) } returns mockCall
    every { mockCall.execute() } returns mockResponse

    // Should not throw any exception
    airbyteKeycloakClient.validateToken(validToken)
  }

  @Test
  fun `validateToken throws MalformedTokenResponseException for token with no sub claim in userinfo`() {
    val validToken = VALID_ACCESS_TOKEN
    val userInfoEndpoint = "http://localhost/realms/master/protocol/openid-connect/userinfo"

    every { keycloakConfiguration.getKeycloakUserInfoEndpointForRealm(any()) } returns userInfoEndpoint

    val mockCall = mockk<Call>()
    val mockResponse =
      okhttp3.Response
        .Builder()
        .request(Request.Builder().url(userInfoEndpoint).build())
        .protocol(Protocol.HTTP_1_1)
        .code(200)
        .message("OK")
        .body("{\"preferred_username\":\"airbyte\"}".toResponseBody())
        .build()

    every { mockHttpClient.newCall(any()) } returns mockCall
    every { mockCall.execute() } returns mockResponse

    assertThrows<MalformedTokenResponseException> {
      airbyteKeycloakClient.validateToken(validToken)
    }
  }

  @Test
  fun `validateToken throws InvalidTokenException for token with no realm`() {
    val blankJWT =
      com.auth0.jwt.JWT
        .create()
        .sign(Algorithm.none())

    assertThrows<InvalidTokenException> {
      airbyteKeycloakClient.validateToken(blankJWT)
    }
  }

  @Test
  fun `validateToken throws InvalidTokenException for token with issuer URL but no realm`() {
    val token =
      com.auth0.jwt.JWT
        .create()
        .withIssuer("https://cloud.airbyte.com")
        .sign(Algorithm.none())

    assertThrows<InvalidTokenException> {
      airbyteKeycloakClient.validateToken(token)
    }

    verify(exactly = 1) { mockMetricClient.count(OssMetricsRegistry.KEYCLOAK_TOKEN_INVALID_REALM, 1) }
  }

  @Test
  fun `validateToken throws TokenExpiredException for 401 response from userinfo endpoint`() {
    val validToken = VALID_ACCESS_TOKEN
    val userInfoEndpoint = "http://localhost/realms/master/protocol/openid-connect/userinfo"

    every { keycloakConfiguration.getKeycloakUserInfoEndpointForRealm(any()) } returns userInfoEndpoint

    val mockCall = mockk<Call>()
    val mockResponse =
      okhttp3.Response
        .Builder()
        .request(Request.Builder().url(userInfoEndpoint).build())
        .protocol(Protocol.HTTP_1_1)
        .code(401)
        .message("Unauthorized")
        .body("".toResponseBody())
        .build()

    every { mockHttpClient.newCall(any()) } returns mockCall
    every { mockCall.execute() } returns mockResponse

    assertThrows<TokenExpiredException> {
      airbyteKeycloakClient.validateToken(validToken)
    }
  }

  @Test
  fun `createOidcSsoConfig clones the configured MCP client into the new realm`() {
    every { keycloakConfiguration.mcpClientId } returns MCP_CLIENT_ID
    every { keycloakConfiguration.mcpClientSourceRealm } returns MCP_SOURCE_REALM

    val config = ssoConfig()
    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val sourceRealmMock = mockk<RealmResource>(relaxed = true)
    val sourceClientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmsMock.realm(MCP_SOURCE_REALM) } returns sourceRealmMock
    every { sourceRealmMock.clients() } returns sourceClientsMock
    every { sourceClientsMock.findByClientId(MCP_CLIENT_ID) } returns listOf(mcpSourceClient())

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(config.companyIdentifier) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    every { clientsMock.findByClientId(MCP_CLIENT_ID) } returns emptyList()
    val capturedClientRepresentations = mutableListOf<ClientRepresentation>()
    every { clientsMock.create(capture(capturedClientRepresentations)) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockResponse
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )

    cloudKeycloakClient().createOidcSsoConfig(config)

    verify(exactly = 3) { clientsMock.create(any()) }
    val mcpClient = capturedClientRepresentations.associateBy { it.clientId }.getValue(MCP_CLIENT_ID)
    assertNull(mcpClient.id)
    assertEquals("Airbyte Cloud MCP", mcpClient.name)
    assertEquals("openid-connect", mcpClient.protocol)
    assertEquals(MCP_CLIENT_SECRET, mcpClient.secret)
    assertEquals(listOf("https://mcp.airbyte.com/auth/callback"), mcpClient.redirectUris)
    assertEquals(listOf("https://mcp.airbyte.com"), mcpClient.webOrigins)
    assertEquals(mapOf("pkce.code.challenge.method" to "S256"), mcpClient.attributes)
    assertEquals(listOf("profile", "email"), mcpClient.defaultClientScopes)
    assertEquals(listOf("offline_access"), mcpClient.optionalClientScopes)
    assertTrue(mcpClient.isEnabled)
    assertFalse(mcpClient.isPublicClient)
    assertTrue(mcpClient.isStandardFlowEnabled)
    // Protocol mappers are cloned, but their source-realm ids are dropped.
    assertEquals(listOf("audience"), mcpClient.protocolMappers.map { it.name })
    assertNull(mcpClient.protocolMappers.single().id)
  }

  @Test
  fun `createOidcSsoConfig skips the MCP client when the client id is blank`() {
    every { keycloakConfiguration.mcpClientId } returns ""

    val config = ssoConfig()
    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    val capturedClientRepresentations = mutableListOf<ClientRepresentation>()
    every { clientsMock.create(capture(capturedClientRepresentations)) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockResponse
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )

    cloudKeycloakClient().createOidcSsoConfig(config)

    verify(exactly = 2) { clientsMock.create(any()) }
    assertFalse(capturedClientRepresentations.any { it.clientId == MCP_CLIENT_ID })
  }

  @Test
  fun `createOidcSsoConfig deletes the realm when the MCP source client is missing`() {
    every { keycloakConfiguration.mcpClientId } returns MCP_CLIENT_ID
    every { keycloakConfiguration.mcpClientSourceRealm } returns MCP_SOURCE_REALM

    val config = ssoConfig()
    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val sourceRealmMock = mockk<RealmResource>(relaxed = true)
    val sourceClientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmsMock.realm(MCP_SOURCE_REALM) } returns sourceRealmMock
    every { sourceRealmMock.clients() } returns sourceClientsMock
    every { sourceClientsMock.findByClientId(MCP_CLIENT_ID) } returns emptyList()

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(config.companyIdentifier) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    every { clientsMock.create(any()) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockResponse
    every { idpMock.importFrom(any()) } returns
      mapOf(
        "authorizationUrl" to "https://auth.airbyte.com/authorize",
        "tokenUrl" to "https://auth.airbyte.com/token",
      )

    assertThrows<CreateClientException> { cloudKeycloakClient().createOidcSsoConfig(config) }

    verify(exactly = 1) { realmMock.remove() }
  }

  @Test
  fun `ensureMcpClientInRealm updates the client when it already exists in the realm`() {
    every { keycloakConfiguration.mcpClientId } returns MCP_CLIENT_ID
    every { keycloakConfiguration.mcpClientSourceRealm } returns MCP_SOURCE_REALM

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val sourceRealmMock = mockk<RealmResource>(relaxed = true)
    val sourceClientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmsMock.realm(MCP_SOURCE_REALM) } returns sourceRealmMock
    every { sourceRealmMock.clients() } returns sourceClientsMock
    every { sourceClientsMock.findByClientId(MCP_CLIENT_ID) } returns listOf(mcpSourceClient())

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm("acme") } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    every { clientsMock.findByClientId(MCP_CLIENT_ID) } returns
      listOf(ClientRepresentation().apply { id = "existing-uuid" })
    val existingClientResource = mockk<ClientResource>(relaxed = true)
    every { clientsMock["existing-uuid"] } returns existingClientResource
    val capturedUpdate = slot<ClientRepresentation>()
    every { existingClientResource.update(capture(capturedUpdate)) } just Runs

    cloudKeycloakClient().ensureMcpClientInRealm("acme")

    verify(exactly = 0) { clientsMock.create(any()) }
    assertEquals("existing-uuid", capturedUpdate.captured.id)
    assertEquals(MCP_CLIENT_ID, capturedUpdate.captured.clientId)
    assertEquals(MCP_CLIENT_SECRET, capturedUpdate.captured.secret)
    assertEquals(listOf("https://mcp.airbyte.com/auth/callback"), capturedUpdate.captured.redirectUris)
  }

  @Test
  fun `ensureMcpClientInRealm reads the secret from the credential endpoint when the representation omits it`() {
    every { keycloakConfiguration.mcpClientId } returns MCP_CLIENT_ID
    every { keycloakConfiguration.mcpClientSourceRealm } returns MCP_SOURCE_REALM

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val sourceRealmMock = mockk<RealmResource>(relaxed = true)
    val sourceClientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmsMock.realm(MCP_SOURCE_REALM) } returns sourceRealmMock
    every { sourceRealmMock.clients() } returns sourceClientsMock
    every { sourceClientsMock.findByClientId(MCP_CLIENT_ID) } returns
      listOf(mcpSourceClient().apply { secret = null })
    val sourceClientResource = mockk<ClientResource>(relaxed = true)
    every { sourceClientsMock["source-uuid"] } returns sourceClientResource
    every { sourceClientResource.secret } returns CredentialRepresentation().apply { value = MCP_CLIENT_SECRET }

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm("acme") } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    every { clientsMock.findByClientId(MCP_CLIENT_ID) } returns emptyList()
    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK
    val capturedCreate = slot<ClientRepresentation>()
    every { clientsMock.create(capture(capturedCreate)) } returns mockResponse

    cloudKeycloakClient().ensureMcpClientInRealm("acme")

    assertEquals(MCP_CLIENT_SECRET, capturedCreate.captured.secret)
  }

  @Test
  fun `ensureMcpClientInRealm throws when the source client is confidential without a readable secret`() {
    every { keycloakConfiguration.mcpClientId } returns MCP_CLIENT_ID
    every { keycloakConfiguration.mcpClientSourceRealm } returns MCP_SOURCE_REALM

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val sourceRealmMock = mockk<RealmResource>(relaxed = true)
    val sourceClientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmsMock.realm(MCP_SOURCE_REALM) } returns sourceRealmMock
    every { sourceRealmMock.clients() } returns sourceClientsMock
    every { sourceClientsMock.findByClientId(MCP_CLIENT_ID) } returns
      listOf(mcpSourceClient().apply { secret = null })
    val sourceClientResource = mockk<ClientResource>(relaxed = true)
    every { sourceClientsMock["source-uuid"] } returns sourceClientResource
    every { sourceClientResource.secret } returns null

    assertThrows<CreateClientException> { cloudKeycloakClient().ensureMcpClientInRealm("acme") }
  }

  @Test
  fun `ensureMcpClientInRealm is a no-op when the client id is blank`() {
    every { keycloakConfiguration.mcpClientId } returns ""

    cloudKeycloakClient().ensureMcpClientInRealm("acme")

    verify(exactly = 0) { keycloakClientMock.realms() }
  }

  @Test
  fun `ensureMcpClientInRealm is a no-op outside Airbyte Cloud`() {
    every { keycloakConfiguration.mcpClientId } returns MCP_CLIENT_ID
    every { keycloakConfiguration.mcpClientSourceRealm } returns MCP_SOURCE_REALM

    // airbyteKeycloakClient is built on the COMMUNITY-edition fixture. Self-managed deployments
    // reach createOidcSsoConfig through the same API but have no source realm to clone from.
    airbyteKeycloakClient.ensureMcpClientInRealm("acme")

    verify(exactly = 0) { keycloakClientMock.realms() }
  }

  private fun cloudKeycloakClient() =
    AirbyteKeycloakClient(
      airbyteKeycloakAdminClientProvider,
      cloudAirbyteConfig,
      keycloakConfiguration,
      mockHttpClient,
      mockMetricClient,
      mockFeatureFlagClient,
    )

  private fun ssoConfig(companyIdentifier: String = "airbyte") =
    SsoConfig(
      organizationId = UUID.randomUUID(),
      emailDomain = "testdomain",
      companyIdentifier = companyIdentifier,
      clientId = "client-id",
      clientSecret = "client-secret",
      discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
      status = SsoConfigStatus.ACTIVE,
    )

  private fun mcpSourceClient() =
    ClientRepresentation().apply {
      id = "source-uuid"
      clientId = MCP_CLIENT_ID
      name = "Airbyte Cloud MCP"
      protocol = "openid-connect"
      secret = MCP_CLIENT_SECRET
      redirectUris = listOf("https://mcp.airbyte.com/auth/callback")
      webOrigins = listOf("https://mcp.airbyte.com")
      attributes = mapOf("pkce.code.challenge.method" to "S256")
      defaultClientScopes = listOf("profile", "email")
      optionalClientScopes = listOf("offline_access")
      protocolMappers =
        listOf(
          ProtocolMapperRepresentation().apply {
            id = "source-mapper-uuid"
            name = "audience"
            protocol = "openid-connect"
          },
        )
      isEnabled = true
      isPublicClient = false
      isStandardFlowEnabled = true
    }

  private fun assertWebappClient(
    client: ClientRepresentation,
    name: String,
    baseUrl: String,
    redirectUris: List<String>,
    webOrigins: List<String>,
  ) {
    assertEquals(name, client.name)
    assertEquals("openid-connect", client.protocol)
    assertEquals(baseUrl, client.baseUrl)
    assertEquals(redirectUris, client.redirectUris)
    assertEquals(webOrigins, client.webOrigins)
    assertTrue(client.isEnabled)
    assertTrue(client.isPublicClient)
    assertTrue(client.isStandardFlowEnabled)
    assertFalse(client.isDirectAccessGrantsEnabled)
    assertFalse(client.isServiceAccountsEnabled)
    assertFalse(client.authorizationServicesEnabled)
    assertFalse(client.isFrontchannelLogout)
    assertFalse(client.isImplicitFlowEnabled)
  }

  companion object {
    private const val MCP_CLIENT_ID = "cloud-mcp"
    private const val MCP_CLIENT_SECRET = "mcp-client-secret"
    private const val MCP_SOURCE_REALM = "_airbyte-cloud-users"

    // Note that this token was specifically constructed to include an underscore
    private val VALID_ACCESS_TOKEN =
      """
      eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIwM095c3pkWmNrZFd6Mk84d0ZFRkZVblJPLVJrN1lGLWZzRm1kWG1Q
      bHdBIn0.eyJleHAiOjE2ODY4MTEwNTAsImlhdCI6MTY4NjgxMDg3MCwiYXV0aF90aW1lIjoxNjg2ODA3MTAzLCJqdGkiOiI1YzZhYTQ0
      Yi02ZDRlLTRkMTktOWQ0NC02YmY0ZjRlMzM5OTYiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwMDAvYXV0aC9yZWFsbXMvbWFzdGVy
      IiwiYXVkIjpbIm1hc3Rlci1yZWFsbSIsImFjY291bnQiXSwic3ViIjoiMGYwY2JmOWEtMjRjMi00NmNjLWI1ODItZDFmZjJjMGQ1ZWY1
      IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYWlyYnl0ZS13ZWJhcHAiLCJzZXNzaW9uX3N0YXRlIjoiN2FhOTdmYTEtYTI1Mi00NmQ0LWE0
      NTMtOTE2Y2E3M2E4NmQ4IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwOi8vbG9jYWxob3N0OjgwMDAiXSwicmVhbG1f
      YWNjZXNzIjp7InJvbGVzIjpbImNyZWF0ZS1yZWFsbSIsImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwib2ZmbGluZV9hY2Nlc3MiLCJhZG1p
      biIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsibWFzdGVyLXJlYWxtIjp7InJvbGVzIjpbInZpZXctaWRl
      bnRpdHktcHJvdmlkZXJzIiwidmlldy1yZWFsbSIsIm1hbmFnZS1pZGVudGl0eS1wcm92aWRlcnMiLCJpbXBlcnNvbmF0aW9uIiwiY3Jl
      YXRlLWNsaWVudCIsIm1hbmFnZS11c2VycyIsInF1ZXJ5LXJlYWxtcyIsInZpZXctYXV0aG9yaXphdGlvbiIsInF1ZXJ5LWNsaWVudHMi
      LCJxdWVyeS11c2VycyIsIm1hbmFnZS1ldmVudHMiLCJtYW5hZ2UtcmVhbG0iLCJ2aWV3LWV2ZW50cyIsInZpZXctdXNlcnMiLCJ2aWV3
      LWNsaWVudHMiLCJtYW5hZ2UtYXV0aG9yaXphdGlvbiIsIm1hbmFnZS1jbGllbnRzIiwicXVlcnktZ3JvdXBzIl19LCJhY2NvdW50Ijp7
      InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9w
      ZW5pZCBwcm9maWxlIGVtYWlsIiwic2lkIjoiN2FhOTdmYTEtYTI1Mi00NmQ0LWE0NTMtOTE2Y2E3M2E4NmQ4IiwiZW1haWxfdmVyaWZp
      ZWQiOmZhbHNlLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhZW_DqyJ9
      """.trimIndent().replace("\n", "").replace("\r", "")
  }

  @Test
  fun `realmExists returns true when the realm can be read`() {
    val realmResource = mockk<RealmResource>()
    every { realmResource.toRepresentation() } returns RealmRepresentation()
    every { keycloakClientMock.realms().realm("acme") } returns realmResource

    assertTrue(airbyteKeycloakClient.realmExists("acme"))
  }

  @Test
  fun `realmExists returns false when the realm cannot be read`() {
    val realmResource = mockk<RealmResource>()
    every { realmResource.toRepresentation() } throws NotFoundException()
    every { keycloakClientMock.realms().realm("acme") } returns realmResource

    assertFalse(airbyteKeycloakClient.realmExists("acme"))
  }

  @Test
  fun `deleteRealm removes the realm`() {
    val realmResource = mockk<RealmResource>()
    every { realmResource.remove() } just Runs
    every { keycloakClientMock.realms().realm("acme") } returns realmResource

    airbyteKeycloakClient.deleteRealm("acme")

    verify(exactly = 1) { realmResource.remove() }
  }

  @Test
  fun `deleteRealm wraps Keycloak failures`() {
    val realmResource = mockk<RealmResource>()
    every { realmResource.remove() } throws RuntimeException("Internal Server Error")
    every { keycloakClientMock.realms().realm("acme") } returns realmResource

    assertThrows<RealmDeletionException> { airbyteKeycloakClient.deleteRealm("acme") }
  }
}
