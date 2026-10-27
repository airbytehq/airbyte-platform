/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.client.auth.AccessTokenInterceptor
import io.airbyte.api.client.config.ApiClientSupportFactory
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.config.Permission
import io.airbyte.data.auth.AirbyteJwtGenerator
import io.airbyte.data.auth.AirbyteJwtGeneratorImpl
import io.airbyte.data.auth.TokenType
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.DataplaneRepository
import io.airbyte.data.repositories.entities.Dataplane
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.data.services.DataplaneTokenService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.ServiceAccountsService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.DataplaneServiceDataImpl
import io.airbyte.data.services.impls.data.DataplaneTokenServiceDataImpl
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toConfigModel
import io.airbyte.domain.models.ServiceAccount
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteInternalApiClientConfig
import io.airbyte.server.services.DataplaneService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.data.repository.PageableRepository
import io.micronaut.runtime.ApplicationConfiguration
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientException
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Base64
import java.util.Optional
import java.util.UUID
import io.airbyte.data.repositories.entities.ServiceAccount as ServiceAccountEntity

class WriteDiscoverCatalogAuthorizationHttpTest : WriteDiscoverCatalogAuthorizationHttpContract() {
  @Test
  fun `production dataplane client exchanges signed credentials caches renews and writes only with saved permission`() {
    withCatalogServer(signed = true) { fixture ->
      val beans = fixture.context.getBean(WriteDiscoverCatalogProducerBeans::class.java)
      val dataplaneId = UUID.randomUUID()
      val groupId = UUID.randomUUID()
      val dataplanes = mockk<DataplaneRepository>()
      val groups = mockk<DataplaneGroupRepository>()
      val grant = slot<Permission>()
      every { dataplanes.existsById(dataplaneId) } returns false
      every { dataplanes.save(any()) } answers { firstArg() }
      every { groups.findById(groupId) } returns
        Optional.of(DataplaneGroup(id = groupId, organizationId = ORGANIZATION, name = "producer", enabled = true, tombstone = false))
      every { fixture.permissions.createServiceAccountPermission(capture(grant)) } answers { firstArg() }
      val created =
        DataplaneServiceDataImpl(dataplanes, groups, fixture.context.getBean(ServiceAccountsService::class.java), fixture.permissions)
          .createDataplaneAndServiceAccount(
            Dataplane(id = dataplaneId, dataplaneGroupId = groupId, name = "producer", enabled = true, tombstone = false).toConfigModel(),
            false,
          )
      beans.credentials = created.serviceAccount
      assertEquals(Permission.PermissionType.DATAPLANE, grant.captured.permissionType)
      assertEquals(ORGANIZATION, grant.captured.organizationId)
      assertEquals(created.serviceAccount.id, grant.captured.serviceAccountId)
      every { fixture.permissions.getPermissionsByServiceAccountId(created.serviceAccount.id) } answers { listOf(grant.captured) }

      val configuration =
        AirbyteInternalApiClientConfig(
          basePath = "${fixture.server.url}/api",
          retries = AirbyteInternalApiClientConfig.RetryConfig(max = 0),
          auth =
            AirbyteInternalApiClientConfig.AuthConfig(
              type = AirbyteInternalApiClientConfig.AuthType.DATAPLANE_ACCESS_TOKEN,
              clientId = created.serviceAccount.id.toString(),
              clientSecret = beans.credentials.secret,
              tokenEndpoint = "${fixture.server.url}/api/v1/dataplanes/token",
            ),
        )
      val application = ApplicationConfiguration().apply { setName("connector-sidecar") }
      val client = ApiClientSupportFactory(application, configuration, mockk(relaxed = true)).airbyteInternalApiClient(AirbyteConfig())
      val request = SourceDiscoverSchemaWriteRequestBody(AirbyteCatalog(emptyList()), SOURCE, "1.2.3", "config-hash")
      assertEquals(CATALOG, client.sourceApi.writeDiscoverCatalogResult(request).catalogId)
      assertEquals(CATALOG, client.sourceApi.writeDiscoverCatalogResult(request).catalogId)
      assertEquals(1, beans.issuedTokens.size, "The injected producer client must reuse its cached token")
      val claims = Jsons.deserialize(String(Base64.getUrlDecoder().decode(beans.issuedTokens.single().split('.')[1])))
      assertEquals(created.serviceAccount.id.toString(), claims["sub"].asText())
      assertEquals(TokenType.DATAPLANE_V1, TokenType.fromClaims(mapOf("typ" to claims["typ"].asText())))
      val interceptor = (client.sourceApi.client as OkHttpClient).interceptors.filterIsInstance<AccessTokenInterceptor>().single()
      interceptor.clock = Clock.fixed(Instant.ofEpochSecond(claims["exp"].asLong()), ZoneOffset.UTC)
      assertEquals(CATALOG, client.sourceApi.writeDiscoverCatalogResult(request).catalogId)
      assertEquals(2, beans.issuedTokens.size, "Expired cached tokens must be exchanged again without sleeping")
      verify(exactly = 3) {
        fixture.catalogs.writeActorCatalogWithFetchEvent(any<io.airbyte.protocol.models.v0.AirbyteCatalog>(), SOURCE, "1.2.3", "config-hash")
      }
      every { fixture.permissions.getPermissionsByServiceAccountId(created.serviceAccount.id) } returns emptyList()
      assertEquals(403, assertThrows<ClientException> { client.sourceApi.writeDiscoverCatalogResult(request) }.statusCode)
      verify(exactly = 3) { fixture.handler.writeDiscoverCatalogResult(any()) }
      verify(exactly = 0) { fixture.permissions.getPermissionsByAuthUserId(any()) }

      // The service-account exchange has its own schema; it is not a producer endpoint substitution.
      every { fixture.permissions.getPermissionsByServiceAccountId(created.serviceAccount.id) } answers { listOf(grant.captured) }
      val serviceAccountResponse =
        post(
          fixture.server,
          "/api/v1/service_accounts/token",
          """{"service_account_id":"${created.serviceAccount.id}","secret":"${beans.credentials.secret}"}""",
        )
      assertEquals(200, serviceAccountResponse.statusCode(), serviceAccountResponse.body())
      val serviceAccountToken = Jsons.deserialize(serviceAccountResponse.body())["access_token"].asText()
      val accountClaims = Jsons.deserialize(String(Base64.getUrlDecoder().decode(serviceAccountToken.split('.')[1])))
      assertEquals(TokenType.SERVICE_ACCOUNT, TokenType.fromClaims(mapOf("typ" to accountClaims["typ"].asText())))
      assertEquals(200, post(fixture.server, "/api/v1/sources/write_discover_catalog_result", catalogBody(), serviceAccountToken).statusCode())
      val (header, payload, signature) = serviceAccountToken.split('.')
      val tampered = "$header.$payload.${if (signature[0] == 'A') 'B' else 'A'}${signature.substring(1)}"
      assertEquals(401, post(fixture.server, "/api/v1/sources/write_discover_catalog_result", catalogBody(), tampered).statusCode())
      val human = fixture.context.getBean(AirbyteJwtGenerator::class.java).generateToken(created.serviceAccount.id.toString(), TokenType.USER, 5)
      assertEquals(403, post(fixture.server, "/api/v1/sources/write_discover_catalog_result", catalogBody(), human).statusCode())
      verify(exactly = 4) { fixture.handler.writeDiscoverCatalogResult(any()) }
      verify(
        exactly = 4,
      ) { fixture.catalogs.writeActorCatalogWithFetchEvent(any<io.airbyte.protocol.models.v0.AirbyteCatalog>(), any(), any(), any()) }
    }
  }
}

@Factory
@Requires(property = "spec.name", value = "WriteDiscoverCatalogAuthorizationHttpTest")
@Requires(property = "test.catalog.signed", value = "true")
class WriteDiscoverCatalogProducerBeans {
  lateinit var credentials: ServiceAccount
  val issuedTokens = mutableListOf<String>()

  @Singleton
  fun authConfig(): AirbyteAuthConfig = AirbyteAuthConfig()

  @Singleton
  fun jwt(
    config: AirbyteAuthConfig,
    generator: JwtTokenGenerator,
  ): AirbyteJwtGenerator = AirbyteJwtGeneratorImpl(config, generator)

  @Singleton
  fun serviceAccounts(
    generator: AirbyteJwtGenerator,
    config: AirbyteAuthConfig,
  ): ServiceAccountsService {
    val constructor = ServiceAccountsService::class.java.declaredConstructors.single { !it.isSynthetic }

    // The account repository is internal to the data module; use its public persistence interface.
    @Suppress("UNCHECKED_CAST")
    val repository = mockkClass(constructor.parameterTypes[1].kotlin) as PageableRepository<ServiceAccountEntity, UUID>
    val saved = slot<ServiceAccountEntity>()
    every { repository.save(capture(saved)) } answers { firstArg() }
    every { repository.findById(any()) } answers {
      val account = saved.captured
      if (firstArg<UUID>() == account.id) Optional.of(account) else Optional.empty()
    }
    val service = constructor.newInstance(generator, repository, config) as ServiceAccountsService
    return service
  }

  @Singleton
  fun dataplaneTokens(
    generator: JwtTokenGenerator,
    config: AirbyteAuthConfig,
    accounts: ServiceAccountsService,
  ): DataplaneTokenService {
    val service = spyk(DataplaneTokenServiceDataImpl(generator, config, accounts))
    every { service.getToken(any(), any()) } answers { callOriginal().also(issuedTokens::add) }
    return service
  }

  @Singleton
  fun dataplanes(
    accounts: ServiceAccountsService,
    tokens: DataplaneTokenService,
  ): DataplaneService = DataplaneService(mockk(), accounts, tokens)

  @Singleton
  fun roles(
    permissions: PermissionService,
    workspaces: WorkspaceService,
  ): RoleResolver {
    val permissionHandler = PermissionHandler(mockk(), workspaces, mockk(), permissions)
    val headers = AuthenticationHeaderResolver(mockk(relaxed = true), permissionHandler, mockk(), mockk(), mockk(), mockk(relaxed = true))
    return RoleResolver(headers, mockk(), mockk(), permissionHandler)
  }

  @Singleton
  fun dataplaneGroups(): DataplaneGroupService = mockk()

  @Singleton
  fun dataplaneHealth(): DataplaneHealthService = mockk()

  @Singleton
  fun entitlements(): EntitlementService = mockk()

  @Singleton
  fun metrics(): MetricClient = mockk(relaxed = true)

  @Singleton
  fun airbyteConfig(): AirbyteConfig = AirbyteConfig()
}
