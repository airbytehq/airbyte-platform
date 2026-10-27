/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.client.auth.AccessTokenInterceptor
import io.airbyte.api.client.config.ApiClientSupportFactory
import io.airbyte.api.client.model.generated.AttemptStats
import io.airbyte.api.client.model.generated.SaveStatsRequestBody
import io.airbyte.api.client.model.generated.SaveStreamAttemptMetadataRequestBody
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.data.auth.AirbyteJwtGenerator
import io.airbyte.data.auth.AirbyteJwtGeneratorImpl
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.data.services.DataplaneTokenService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.ServiceAccountsService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.DataplaneTokenServiceDataImpl
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

class AttemptWriteAuthorizationHttpTest : AttemptWriteAuthorizationHttpContract() {
  @Test
  fun `orchestrator stats client exchanges signed credentials caches renews and requires saved grants`() {
    producerExchange(metadata = false)
  }

  @Test
  fun `init container metadata client exchanges signed credentials caches renews and requires saved grants`() {
    producerExchange(metadata = true)
  }

  private fun producerExchange(metadata: Boolean) {
    withAttemptServer(signed = true) { fixture ->
      val beans = fixture.context.getBean(AttemptWriteProducerBeans::class.java)
      val configuration =
        AirbyteInternalApiClientConfig(
          basePath = "${fixture.server.url}/api",
          retries = AirbyteInternalApiClientConfig.RetryConfig(max = 0),
          auth =
            AirbyteInternalApiClientConfig.AuthConfig(
              type = AirbyteInternalApiClientConfig.AuthType.DATAPLANE_ACCESS_TOKEN,
              clientId = ACCOUNT.toString(),
              clientSecret = beans.credentials.secret,
              tokenEndpoint = "${fixture.server.url}/api/v1/dataplanes/token",
            ),
        )
      val application = ApplicationConfiguration().apply { setName(if (metadata) "workload-init-container" else "container-orchestrator") }
      val client = ApiClientSupportFactory(application, configuration, mockk(relaxed = true)).airbyteInternalApiClient(AirbyteConfig())
      // Match the unchanged production serializers: stats supplies connectionId; metadata has no such field.
      val stats =
        SaveStatsRequestBody(
          JOB,
          ATTEMPT,
          AttemptStats(
            recordsEmitted = 0,
            bytesEmitted = 0,
            recordsCommitted = 0,
            bytesCommitted = 0,
            recordsRejected = 0,
            estimatedRecords = 0,
            estimatedBytes = 0,
          ),
          emptyList(),
          CONNECTION,
        )
      val streams = SaveStreamAttemptMetadataRequestBody(JOB, ATTEMPT, emptyList())

      fun write(): Boolean = if (metadata) client.attemptApi.saveStreamMetadata(streams).succeeded else client.attemptApi.saveStats(stats).succeeded
      assertEquals(true, write())
      assertEquals(true, write())
      assertEquals(1, beans.issuedTokens.size, "The production client must reuse its cached token")
      val claims = Jsons.deserialize(String(Base64.getUrlDecoder().decode(beans.issuedTokens.single().split('.')[1])))
      assertEquals(ACCOUNT.toString(), claims["sub"].asText())
      assertEquals(TokenType.DATAPLANE_V1, TokenType.fromClaims(mapOf("typ" to claims["typ"].asText())))
      val interceptor = (client.attemptApi.client as OkHttpClient).interceptors.filterIsInstance<AccessTokenInterceptor>().single()
      interceptor.clock = Clock.fixed(Instant.ofEpochSecond(claims["exp"].asLong()), ZoneOffset.UTC)
      assertEquals(true, write())
      assertEquals(2, beans.issuedTokens.size, "Expired token must trigger the real credential exchange again")
      every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns emptyList()
      assertEquals(403, assertThrows<ClientException> { write() }.statusCode)
      verify(exactly = 0) { fixture.permissions.getPermissionsByAuthUserId(any()) }
      every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns listOf(grant("organization"))

      // Service-account credentials have a different request schema from the production dataplane exchange.
      val response =
        post(fixture.server, "/api/v1/service_accounts/token", """{"service_account_id":"$ACCOUNT","secret":"${beans.credentials.secret}"}""")
      assertEquals(200, response.statusCode(), response.body())
      val token = Jsons.deserialize(response.body())["access_token"].asText()
      val accountClaims = Jsons.deserialize(String(Base64.getUrlDecoder().decode(token.split('.')[1])))
      assertEquals(TokenType.SERVICE_ACCOUNT, TokenType.fromClaims(mapOf("typ" to accountClaims["typ"].asText())))
      val path = "/api/v1/attempt/${if (metadata) "save_stream_metadata" else "save_stats"}"
      val request = if (metadata) metadataBody() else statsBody()
      assertEquals(200, post(fixture.server, path, request, token).statusCode())
      val (header, payload, signature) = token.split('.')
      val tampered = "$header.$payload.${if (signature[0] == 'A') 'B' else 'A'}${signature.substring(1)}"
      assertEquals(401, post(fixture.server, path, request, tampered).statusCode())
      val human = fixture.context.getBean(AirbyteJwtGenerator::class.java).generateToken(ACCOUNT.toString(), TokenType.USER, 5)
      assertEquals(403, post(fixture.server, path, request, human).statusCode())
      verify(exactly = if (metadata) 0 else 4) { fixture.handler.saveStats(any()) }
      verify(exactly = if (metadata) 4 else 0) { fixture.handler.saveStreamMetadata(any()) }
      verify(
        exactly = if (metadata) 0 else 4,
      ) { fixture.persistence.writeStats(JOB, ATTEMPT, 0, 0, 0, 0, 0, 0, 0, CONNECTION, match { it.isEmpty() }) }
      verify(exactly = if (metadata) 4 else 0) { fixture.metadata.saveAll(any<List<io.airbyte.data.repositories.entities.StreamAttemptMetadata>>()) }
    }
  }
}

@Factory
@Requires(property = "spec.name", value = "AttemptWriteAuthorizationHttpTest")
@Requires(property = "test.attempt.signed", value = "true")
class AttemptWriteProducerBeans {
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
    every { repository.findById(AttemptWriteAuthorizationHttpContract.ACCOUNT) } answers { Optional.of(saved.captured) }
    val service = constructor.newInstance(generator, repository, config) as ServiceAccountsService
    credentials = service.create(AttemptWriteAuthorizationHttpContract.ACCOUNT, "attempt-producer", true)
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
