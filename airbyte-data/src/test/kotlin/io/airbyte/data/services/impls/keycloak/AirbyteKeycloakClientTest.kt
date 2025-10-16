/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import com.auth0.jwt.algorithms.Algorithm
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigStatus
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.ws.rs.core.Response
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.IdentityProviderResource
import org.keycloak.admin.client.resource.IdentityProvidersResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.RealmsResource
import org.keycloak.representations.idm.IdentityProviderRepresentation
import java.util.UUID

class AirbyteKeycloakClientTest {
  private val airbyteUrl: String = "https://cloud.airbyte.com"
  private val airbyteConfig: AirbyteConfig = AirbyteConfig(airbyteUrl = airbyteUrl)
  private val keycloakConfiguration: AirbyteKeycloakConfig = mockk<AirbyteKeycloakConfig>(relaxed = true)
  private lateinit var airbyteKeycloakAdminClientProvider: AirbyteKeycloakAdminClientProvider
  private lateinit var airbyteKeycloakClient: AirbyteKeycloakClient
  private lateinit var mockHttpClient: OkHttpClient

  private var keycloakClientMock = mockk<Keycloak>(relaxed = true)

  @BeforeEach
  fun setup() {
    airbyteKeycloakAdminClientProvider = mockk<AirbyteKeycloakAdminClientProvider>(relaxed = true)
    mockHttpClient = mockk<OkHttpClient>()
    every { airbyteKeycloakAdminClientProvider.createKeycloakAdminClient() } returns keycloakClientMock
    airbyteKeycloakClient = AirbyteKeycloakClient(airbyteKeycloakAdminClientProvider, airbyteConfig, keycloakConfiguration, mockHttpClient)
  }

  @AfterEach
  fun tearDown() {
    clearMocks(keycloakClientMock)
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
    every { clientsMock.create(any()) } returns mockResponse

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

    verify(exactly = 4) { keycloakClientMock.realms() }
    verify(exactly = 1) { realmsMock.create(any()) }
    verify(exactly = 2) { realmMock.identityProviders() }
    verify(exactly = 1) { idpMock.create(any()) }
    verify(exactly = 1) { realmMock.clients() }
    verify(exactly = 1) { clientsMock.create(any()) }
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

  companion object {
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
}
