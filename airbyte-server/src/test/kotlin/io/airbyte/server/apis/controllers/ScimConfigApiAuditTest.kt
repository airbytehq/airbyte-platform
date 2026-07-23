/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.airbyte.api.server.generated.models.EnableScimRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.ScimConfigResponse
import io.airbyte.api.server.generated.models.ScimIdpProvider
import io.airbyte.audit.logging.AuditLoggingInterceptor
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimConfigurationStatus
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.airbyte.server.assertStatus
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ShutdownEvent
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.UUID
import io.airbyte.domain.models.scim.ScimIdpProvider as DomainScimIdpProvider

private const val SCIM_AUDIT_SPEC = "ScimConfigApiAuditTest"
private const val RAW_TOKEN = "airbyte_scim_audit_token"
private const val TOKEN_HASH = "e58b3b847d8d91a60d8a83a13bbf62dc06a4d55f9a17b9cf226e1fd26e37cc01"
private const val FAILURE_RAW_TOKEN = "airbyte_scim_failure_token"
private const val FAILURE_TOKEN_HASH = "failure_token_hash_sentinel"
private const val FAILURE_MESSAGE = "Database rejected token $FAILURE_RAW_TOKEN with hash $FAILURE_TOKEN_HASH"
private val AUDIT_STORAGE_CLIENT = mockk<StorageClient>(relaxed = true)

@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SCIM_AUDIT_SPEC)
@Property(name = "airbyte.audit.logging.enabled", value = "true")
@Property(name = "micronaut.security.enabled", value = "false")
internal class ScimConfigApiAuditTest {
  @Requires(property = "spec.name", value = SCIM_AUDIT_SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun scimConfigurationService(): ScimConfigurationService = mockk()

    @Singleton
    @Primary
    fun currentUserService(): CurrentUserService = mockk()

    @Singleton
    @Replaces(StorageClientFactory::class)
    fun storageClientFactory(): StorageClientFactory =
      mockk {
        every { create(any()) } returns AUDIT_STORAGE_CLIENT
      }
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var service: ScimConfigurationService

  @Inject
  lateinit var currentUserService: CurrentUserService

  @Inject
  lateinit var auditLoggingInterceptor: AuditLoggingInterceptor

  private val organizationId = UUID.fromString("c7838139-e08d-4493-9785-2a626531e9bd")
  private val userId = UUID.fromString("fcb3d9b6-b40f-45d1-8dd5-6328c2ed18fc")
  private val auditDocuments = mutableListOf<String>()

  @BeforeEach
  fun setUp() {
    clearMocks(service, currentUserService, AUDIT_STORAGE_CLIENT)
    auditDocuments.clear()
    every { currentUserService.getCurrentUser() } returns
      mockk(relaxed = true) {
        every { this@mockk.userId } returns this@ScimConfigApiAuditTest.userId
      }
    every { AUDIT_STORAGE_CLIENT.write(any(), any()) } answers {
      if (firstArg<String>().startsWith("audit-logging")) {
        auditDocuments.add(secondArg())
      }
    }
  }

  @Test
  fun `success and failure paths emit sanitized audit entries through the live pipeline`() {
    every {
      service.enable(
        OrganizationId(organizationId),
        DomainScimIdpProvider.OKTA,
        UserId(userId),
      )
    } returns
      ScimConfigurationRead(
        status = ScimConfigurationStatus.ENABLED,
        idpProvider = DomainScimIdpProvider.OKTA,
        token = RAW_TOKEN,
      )
    every { service.disable(OrganizationId(organizationId), UserId(userId)) } just Runs

    val enableResponse =
      client.toBlocking().exchange(
        HttpRequest.POST(
          "/api/v1/scim_config/enable",
          EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA),
        ),
        ScimConfigResponse::class.java,
      )
    val disableResponse =
      client.toBlocking().exchange<OrganizationIdRequestBody, Any>(
        HttpRequest.POST(
          "/api/v1/scim_config/disable",
          OrganizationIdRequestBody(organizationId),
        ),
      )

    assertStatus(HttpStatus.OK, enableResponse.status)
    assertThat(enableResponse.body()!!.token).isEqualTo(RAW_TOKEN)
    assertStatus(HttpStatus.NO_CONTENT, disableResponse.status)

    every {
      service.enable(
        OrganizationId(organizationId),
        DomainScimIdpProvider.OKTA,
        UserId(userId),
      )
    } throws IllegalStateException(FAILURE_MESSAGE)

    val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) as Logger
    val logAppender =
      ListAppender<ILoggingEvent>().apply {
        context = rootLogger.loggerContext
        start()
      }
    rootLogger.addAppender(logAppender)
    try {
      val exception =
        assertThrows(HttpClientResponseException::class.java) {
          client.toBlocking().exchange<EnableScimRequestBody, Any>(
            HttpRequest.POST(
              "/api/v1/scim_config/enable",
              EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA),
            ),
          )
        }
      assertStatus(HttpStatus.INTERNAL_SERVER_ERROR, exception.status)
    } finally {
      rootLogger.detachAppender(logAppender)
      logAppender.stop()
    }

    auditLoggingInterceptor.onShutdownEvent(mockk<ShutdownEvent>())

    verify(exactly = 1) { AUDIT_STORAGE_CLIENT.write(any(), any()) }
    assertThat(auditDocuments).hasSize(1)
    val storedDocument = auditDocuments.single()
    val auditEntries = Jsons.deserialize(storedDocument)
    assertThat(auditEntries).hasSize(3)

    val enableEntry = auditEntries.single { it["operation"].asText() == "enableScim" && it["success"].asBoolean() }
    val enableRequest = Jsons.deserialize(enableEntry["request"].asText())
    assertThat(enableEntry["success"].asBoolean()).isTrue()
    assertThat(enableRequest.fieldNames().asSequence().toSet()).containsExactlyInAnyOrder("organizationId", "idpProvider")
    assertThat(enableRequest["organizationId"].asText()).isEqualTo(organizationId.toString())
    assertThat(enableRequest["idpProvider"].asText()).isEqualTo("okta")
    assertThat(Jsons.deserialize(enableEntry["response"].asText()).isEmpty).isTrue()

    val disableEntry = auditEntries.single { it["operation"].asText() == "disableScim" }
    val disableRequest = Jsons.deserialize(disableEntry["request"].asText())
    assertThat(disableEntry["success"].asBoolean()).isTrue()
    assertThat(disableRequest.fieldNames().asSequence().toSet()).containsExactly("organizationId")
    assertThat(disableRequest["organizationId"].asText()).isEqualTo(organizationId.toString())
    assertThat(Jsons.deserialize(disableEntry["response"].asText()).isEmpty).isTrue()

    val failedEnableEntry = auditEntries.single { it["operation"].asText() == "enableScim" && !it["success"].asBoolean() }
    val failedEnableRequest = Jsons.deserialize(failedEnableEntry["request"].asText())
    assertThat(failedEnableRequest.fieldNames().asSequence().toSet()).containsExactlyInAnyOrder("organizationId", "idpProvider")
    assertThat(failedEnableEntry["response"].isNull).isTrue()
    assertThat(failedEnableEntry["errorMessage"].asText()).isEqualTo("SCIM configuration operation failed")

    val uncaughtLog = logAppender.list.single { it.formattedMessage.contains("Uncaught exception") }
    assertThat(uncaughtLog.formattedMessage).contains("SCIM configuration operation failed")
    assertThat(uncaughtLog.formattedMessage).doesNotContain(FAILURE_RAW_TOKEN, FAILURE_TOKEN_HASH, FAILURE_MESSAGE)

    assertThat(storedDocument)
      .doesNotContain(
        RAW_TOKEN,
        TOKEN_HASH,
        FAILURE_RAW_TOKEN,
        FAILURE_TOKEN_HASH,
        FAILURE_MESSAGE,
        "tokenHash",
        "token_hash",
        "\"token\"",
      )
  }
}
