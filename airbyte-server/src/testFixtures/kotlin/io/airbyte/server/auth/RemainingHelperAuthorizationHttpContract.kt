/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.CheckOperationRead
import io.airbyte.api.server.generated.models.DeclarativeManifestBaseImageRead
import io.airbyte.api.server.generated.models.DeploymentMetadataRead
import io.airbyte.api.server.generated.models.NotificationRead
import io.airbyte.api.server.generated.models.NotificationTrigger
import io.airbyte.api.server.generated.models.WebBackendCheckUpdatesRead
import io.airbyte.api.server.generated.models.WebBackendCronExpressionDescription
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.builder.contributions.ContributionCreateResult
import io.airbyte.commons.server.builder.contributions.ContributionRead
import io.airbyte.commons.server.handlers.AssistProxyHandler
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler
import io.airbyte.commons.server.handlers.ConnectorContributionHandler
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler
import io.airbyte.commons.server.handlers.NotificationsHandler
import io.airbyte.commons.server.handlers.OpenApiConfigHandler
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.data.services.PermissionService
import io.airbyte.server.apis.controllers.ConnectorBuilderAssistApiController
import io.airbyte.server.apis.controllers.ConnectorBuilderProjectApiController
import io.airbyte.server.apis.controllers.DeploymentMetadataApiController
import io.airbyte.server.apis.controllers.NotificationsApiController
import io.airbyte.server.apis.controllers.OpenapiApiController
import io.airbyte.server.apis.controllers.OperationApiController
import io.airbyte.server.apis.controllers.WebBackendApiController
import io.airbyte.server.handlers.WebBackendCronExpressionHandler
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.nio.file.Files
import java.util.UUID

abstract class RemainingHelperAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `authentication-only helper rejects unsupported worker despite administrator label and earlier allowed`() {
    exercise(scenarios().first(), "workload", 403)
  }

  @Test
  fun `all eleven helper operations use their independently expected rule and normal response`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, "user", scenario.status) } })
  }

  @Test
  fun `all eleven helpers reject before business after earlier allowed`() {
    assertAll(
      scenarios().map { scenario ->
        Executable {
          exercise(scenario, if (scenario.rule == DEPLOYMENT_RULE) "scoped:${UUID(0, 1)}" else "workload", 403)
        }
      },
    )
  }

  @Test
  fun `authentication-only metadata keeps supported callers but cannot use a workspace token without an owner`() {
    val assist = scenarios().first()
    for (token in listOf("internal", "dataplane", "service")) exercise(assist, token, 200)
    for (token in listOf("embedded", "scoped:${UUID(0, 1)}")) exercise(assist, token, 403)
    val deployment = scenarios().single { it.rule == DEPLOYMENT_RULE }
    for (token in listOf("internal", "dataplane", "service", "embedded", "workload")) exercise(deployment, token, 200)
  }

  @Test
  fun `no auth helper execution skips all permission access`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, "user", scenario.status, security = false) } })
  }

  private fun exercise(
    scenario: Scenario,
    token: String,
    expected: Int,
    security: Boolean = true,
  ) {
    val web = if (cloud) Class.forName("io.airbyte.server.wrapped.apis.WebBackendApiWrappedController") else WebBackendApiController::class.java
    val operation = if (cloud) Class.forName("io.airbyte.server.wrapped.apis.OperationApiWrappedController") else OperationApiController::class.java
    val controllers =
      listOf(
        ConnectorBuilderAssistApiController::class.java,
        DeploymentMetadataApiController::class.java,
        NotificationsApiController::class.java,
        OpenapiApiController::class.java,
        operation,
        OperationApiController::class.java,
        ConnectorBuilderProjectApiController::class.java,
        web,
        WebBackendApiController::class.java,
      ).distinct()
    val specification = Files.createTempFile("remaining-authorization-openapi", ".json")
    Files.writeString(specification, """{"openapi":"3.0.0","info":{"title":"fixture","version":"1"},"paths":{}}""")
    val grants = mockk<PermissionService>()
    try {
      RemainingOperationHttpSupport(cloud, controllers, setOf(RULE, DEPLOYMENT_RULE), security).run(
        configure = { context, business ->
          business.forEach { instance ->
            when (instance) {
              is AssistProxyHandler -> every { instance.process(mapOf("prompt" to "fixture"), true) } returns mapOf("result" to "fixture")
              is DeploymentMetadataHandler ->
                every { instance.getDeploymentMetadata() } returns
                  DeploymentMetadataRead(UUID(0, 1), if (cloud) "CLOUD" else "OSS", "fixture")
              is NotificationsHandler ->
                every {
                  instance.tryNotification(match { it.webhook == "https://example.com/fixture" }, NotificationTrigger.SYNC_SUCCESS)
                } returns NotificationRead(NotificationRead.Status.SUCCEEDED)
              is OpenApiConfigHandler -> every { instance.file } returns specification.toFile()
              is OperationsHandler ->
                every {
                  instance.checkOperation(match { it.operatorType == io.airbyte.api.server.generated.models.OperatorType.WEBHOOK })
                } returns CheckOperationRead(status = CheckOperationRead.Status.SUCCEEDED)
              is ConnectorBuilderProjectsHandler ->
                every { instance.getDeclarativeManifestBaseImage(match { it.manifest.isObject }) } returns
                  DeclarativeManifestBaseImageRead("fixture:1")
              is ConnectorContributionHandler -> {
                every {
                  instance.generateContribution(
                    match {
                      it.name == "fixture" && it.connectorImageName == "source-fixture" &&
                        it.githubToken == "fixture"
                    },
                  )
                } returns ContributionCreateResult("https://example.com/fixture", UUID(0, 1))
                every { instance.checkContribution("source-fixture") } returns ContributionRead(false)
              }
              is WebBackendCheckUpdatesHandler ->
                every { instance.checkUpdates() } returns
                  WebBackendCheckUpdatesRead(sourceDefinitions = 0, destinationDefinitions = 0)
              is WebBackendCronExpressionHandler ->
                every { instance.describeCronExpression(match { it.cronExpression == "0 * * * * ?" }) } returns
                  WebBackendCronExpressionDescription(cronExpression = "0 * * * * ?", description = "fixture", nextExecutions = emptyList())
            }
          }
          context.registerSingleton(
            OperationPermissions::class.java,
            OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
          )
        },
        check = { context, server, business ->
          RemainingOperationHttpSupport.requireRule(context, scenario.method, scenario.path, scenario.rule)
          val response = RemainingOperationHttpSupport.request(server, scenario.method, scenario.path, scenario.body, token)
          assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
          if (expected == 403) RemainingOperationHttpSupport.requireNoBusiness(business)
          if (expected == 200) assertTrue(response.body().isNotBlank(), scenario.path)
          verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
          verify(exactly = 0) { grants.getPermissionsByServiceAccountId(any()) }
        },
      )
    } finally {
      Files.deleteIfExists(specification)
    }
  }

  private data class Scenario(
    val path: String,
    val body: String = "{}",
    val method: String = "POST",
    val rule: String = RULE,
    val status: Int = 200,
  )

  private fun scenarios() =
    listOf(
      Scenario("/api/v1/connector_builder_assist/process", """{"prompt":"fixture"}"""),
      Scenario("/api/v1/deployment/metadata", rule = DEPLOYMENT_RULE),
      Scenario("/api/v1/notifications/try", """{"notificationType":"slack"}""", status = 422),
      Scenario(
        "/api/v1/notifications/try_webhook",
        """{"slackConfiguration":{"webhook":"https://example.com/fixture"},"notificationTrigger":"sync_success"}""",
      ),
      Scenario("/api/v1/openapi", method = "GET"),
      Scenario("/api/v1/operations/check", """{"operatorType":"webhook"}"""),
      Scenario("/api/v1/connector_builder_projects/get_base_image", """{"manifest":{}}"""),
      Scenario(
        "/api/v1/connector_builder_projects/contribute/generate",
        """{"name":"fixture","connector_image_name":"source-fixture","connector_description":"fixture","github_token":"fixture","manifest_yaml":"version: 1","base_image":"fixture:1","contribution_description":"fixture"}""",
      ),
      Scenario("/api/v1/connector_builder_projects/contribute/check", """{"connector_image_name":"source-fixture"}"""),
      Scenario("/api/v1/web_backend/check_updates"),
      Scenario("/api/v1/web_backend/describe_cron_expression", """{"cronExpression":"0 * * * * ?"}"""),
    )

  companion object {
    const val RULE = "io.airbyte.server.auth.AuthenticatedMetadataRule"
    const val DEPLOYMENT_RULE = "io.airbyte.server.auth.DeploymentMetadataRule"
  }
}
