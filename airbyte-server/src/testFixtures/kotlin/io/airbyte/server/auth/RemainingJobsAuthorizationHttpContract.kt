/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.BooleanRead
import io.airbyte.api.server.generated.models.CheckDataWorkerCapacityRead
import io.airbyte.api.server.generated.models.InternalOperationResult
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.JobInputHandler
import io.airbyte.commons.server.handlers.JobsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.temporal.StreamResetRecordsHelper
import io.airbyte.config.JobWebhookConfig
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.AttemptApiController
import io.airbyte.server.apis.controllers.JobRetryStatesApiController
import io.airbyte.server.apis.controllers.JobsApiController
import io.airbyte.server.handlers.RetryStatesHandler
import io.airbyte.server.services.JobObservabilityService
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.micronaut.web.router.Router
import io.mockk.Called
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.verify
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID

abstract class RemainingJobsAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `job failure rejects another connection even for an instance administrator`() =
    request("/api/v1/jobs/job_failure", """{"jobId":7,"connectionId":"$OTHER_CONNECTION","attemptNumber":0,"reason":"test"}""", 400)

  @Test
  fun `every jobs attempt and retry-state operation executes its independently expected rule and normal business response`() {
    org.junit.jupiter.api.Assertions.assertAll(
      scenarios().map { scenario ->
        org.junit.jupiter.api.function.Executable {
          request(
            scenario.path,
            scenario.body,
            if (!cloud &&
              scenario.path.endsWith("/explain")
            ) {
              501
            } else {
              scenario.status
            },
            scenario.rule,
            scenario.grant,
            responseBody = scenario.responseBody,
            token = scenario.token,
          )
        }
      },
    )
  }

  @Test
  fun `every jobs attempt and retry-state operation denies before all business work after earlier allowed`() {
    org.junit.jupiter.api.Assertions.assertAll(
      scenarios().map { scenario ->
        org.junit.jupiter.api.function.Executable {
          request(scenario.path, scenario.body, 403, scenario.rule, scenario.grant, granted = false, token = scenario.token)
        }
      },
    )
  }

  @Test
  fun `worker administrative operations reject human administrators despite grants`() {
    org.junit.jupiter.api.Assertions.assertAll(
      scenarios().filter { it.path in WORKER_PATHS }.map { scenario ->
        org.junit.jupiter.api.function.Executable {
          for (token in listOf(
            "valid",
            "application-owner",
          )) {
            request(scenario.path, scenario.body, 403, scenario.rule, Permission.PermissionType.INSTANCE_ADMIN, token = token)
          }
        }
      },
    )
  }

  @Test
  fun `supported internal workers execute with their explicit caller grant while service accounts still need the action`() {
    org.junit.jupiter.api.Assertions.assertAll(
      scenarios().filter { it.path in WORKER_PATHS }.map { scenario ->
        org.junit.jupiter.api.function.Executable {
          request(
            scenario.path,
            scenario.body,
            scenario.status,
            scenario.rule,
            scenario.grant,
            granted = false,
            responseBody = scenario.responseBody,
            token = "internal",
          )
          if (scenario.rule !=
            JobInputRule::class.java
          ) {
            request(scenario.path, scenario.body, 403, scenario.rule, Permission.PermissionType.WORKSPACE_READER, token = "service")
          }
        }
      },
    )
  }

  @Test
  fun `dataplane input and last-job reads retain their required saved workspace grants`() {
    for (scenario in scenarios().filter { it.path.endsWith("/get_input") || it.path.endsWith("/get_last_replication_job") }) {
      request(scenario.path, scenario.body, 200, scenario.rule, scenario.grant, token = "dataplane")
      request(scenario.path, scenario.body, 403, scenario.rule, scenario.grant, granted = false, token = "dataplane")
      request(scenario.path, scenario.body, 403, scenario.rule, Permission.PermissionType.INSTANCE_ADMIN, token = "service-other-scope")
    }
  }

  @Test
  fun `job connection organization and local attempt relationships reject before business`() {
    request("/api/v1/jobs/checkDataWorkerCapacity", """{"jobId":7,"connectionId":"$CONNECTION","organizationId":"${UUID(0, 99)}"}""", 400)
    request(
      "/api/v1/jobs/retry_states/create_or_update",
      """{"jobId":7,"connectionId":"$OTHER_CONNECTION","successiveCompleteFailures":0,"totalCompleteFailures":0,"successivePartialFailures":0,"totalPartialFailures":0}""",
      400,
    )
    request("/api/v1/attempt/get_for_job", """{"jobId":7,"attemptNumber":1}""", 404, JobReadRule::class.java)
  }

  @Test
  fun `all supplied workspaces are required and duplicates preserve reader behavior`() {
    request(
      "/api/v1/jobs/list_for_workspaces",
      """{"configTypes":["sync"],"workspaceIds":["$WORKSPACE","$WORKSPACE"]}""",
      200,
      JobReadRule::class.java,
      Permission.PermissionType.WORKSPACE_READER,
    )
    request(
      "/api/v1/jobs/list_for_workspaces",
      """{"configTypes":["sync"],"workspaceIds":["$WORKSPACE","$OTHER_WORKSPACE"]}""",
      403,
      JobReadRule::class.java,
      Permission.PermissionType.WORKSPACE_READER,
    )
  }

  @Test
  fun `empty workspace list is an explicit administrator query rather than an ordinary reader fallback`() {
    val body = """{"configTypes":["sync"],"workspaceIds":[]}"""
    request("/api/v1/jobs/list_for_workspaces", body, 403, JobReadRule::class.java, Permission.PermissionType.WORKSPACE_READER)
    request("/api/v1/jobs/list_for_workspaces", body, 200, JobReadRule::class.java)
  }

  @Test
  fun `nonreplication scopes keep their explicit global administrator policy`() {
    val body = """{"configTypes":["get_spec"],"configId":"opaque"}"""
    request("/api/v1/jobs/list", body, 403, JobReadRule::class.java, Permission.PermissionType.WORKSPACE_READER)
    request("/api/v1/jobs/list", body, 200, JobReadRule::class.java)
  }

  @Test
  fun `including job anchor from outside the selected connection keeps empty-page semantics`() =
    request(
      "/api/v1/jobs/list",
      """{"configTypes":["sync"],"configId":"$CONNECTION","includingJobId":99}""",
      200,
      JobReadRule::class.java,
      Permission.PermissionType.WORKSPACE_READER,
    )

  @Test
  fun `null missing zero saved ids and ownership errors never run business`() {
    for (body in listOf("{}", "null", """{"id":null}""")) request("/api/v1/jobs/cancel", body, 400, JobRunRule::class.java)
    request("/api/v1/jobs/cancel", """{"id":0}""", 400, JobRunRule::class.java)
    request("/api/v1/jobs/cancel", """{"id":7}""", 500, JobRunRule::class.java, dataFailure = true)
  }

  @Test
  fun `unsupported callers and different token workspace reject despite administrator role labels`() {
    for (token in listOf(
      "workload",
      "embedded",
      "other-scope",
    )) {
      request("/api/v1/jobs/cancel", """{"id":7}""", 403, JobRunRule::class.java, token = token)
    }
  }

  @Test
  fun `no auth preserves valid zero attempt and still rejects a mismatched job relationship`() {
    request("/api/v1/attempt/get_for_job", """{"jobId":7,"attemptNumber":0}""", 200, JobReadRule::class.java, security = false, granted = false)
    request(
      "/api/v1/jobs/job_failure",
      """{"jobId":7,"connectionId":"$OTHER_CONNECTION","attemptNumber":0,"reason":"test"}""",
      400,
      security = false,
      granted = false,
    )
  }

  @Test
  fun `OSS job explanation stub preserves no auth unsupported response without saved job reads`() {
    if (cloud) return
    request("/api/v1/jobs/explain", """{"id":0}""", 501, JobExplainUnsupportedRule::class.java, security = false, granted = false, unusedOwner = true)
    request(
      "/api/v1/jobs/explain",
      """{"id":7}""",
      501,
      JobExplainUnsupportedRule::class.java,
      security = false,
      granted = false,
      dataFailure = true,
      unusedOwner = true,
    )
  }

  @Test
  fun `OSS job explanation preserves static instance admin and missing id no auth requests`() {
    if (cloud) return
    org.junit.jupiter.api.Assertions.assertAll(
      { request("/api/v1/jobs/explain", """{"id":0}""", 501, JobExplainUnsupportedRule::class.java, token = "valid", unusedOwner = true) },
      { request("/api/v1/jobs/explain", "{}", 400, JobExplainUnsupportedRule::class.java, security = false, granted = false, unusedOwner = true) },
    )
  }

  protected inline fun <reified T> responseFixture(): T {
    val json =
      RemainingJobsAuthorizationHttpContract::class.java.getResourceAsStream("/auth/remaining-job-responses.json")!!.use {
        it.readBytes().toString(Charsets.UTF_8)
      }
    return MoreMappers.initMapper().treeToValue(Jsons.deserialize(json)[T::class.java.simpleName], T::class.java)
  }

  private fun request(
    path: String,
    body: String,
    status: Int,
    rule: Class<*> = JobAdminRule::class.java,
    grant: Permission.PermissionType = Permission.PermissionType.INSTANCE_ADMIN,
    granted: Boolean = true,
    responseBody: Boolean = true,
    token: String = "service",
    security: Boolean = true,
    dataFailure: Boolean = false,
    unusedOwner: Boolean = false,
  ) {
    val controller =
      if (cloud) {
        Class.forName(
          "io.airbyte.server.wrapped.apis.controllers.JobsApiWrappedController",
        )
      } else {
        JobsApiController::class.java
      }
    val controllers =
      listOf(
        controller,
        JobsApiController::class.java,
        AttemptApiController::class.java,
        JobRetryStatesApiController::class.java,
      ).distinct()
    val allowed =
      listOf(
        *controllers.map { it.name }.toTypedArray(),
        "io.airbyte.server.auth.RemainingJobs",
        "io.airbyte.server.auth.Job",
        "io.airbyte.server.auth.AuthorizedRequest",
        "io.airbyte.server.auth.AuthorizationInterceptor",
        "io.airbyte.server.auth.RouteAccess",
        "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
        "io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler",
      )
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
        override fun isWithin(reference: BeanDefinitionReference<*>): Boolean = isWithin(reference.beanDefinitionName.replace("$", ""))

        override fun isWithin(type: Class<*>): Boolean = isWithin(type.name)

        override fun isWithin(name: String): Boolean = name.startsWith("io.airbyte.") && allowed.none(name::startsWith)
      }
    ApplicationContext
      .builder()
      .enableDefaultPropertySources(false)
      .deduceEnvironment(false)
      .environmentPropertySource(false)
      .environments(if (cloud) "cloud" else "test")
      .beanConfigurations(isolated)
      .properties(
        mapOf(
          "spec.name" to "RemainingJobsAuthorizationHttpTest",
          "micronaut.security.enabled" to security,
          "micronaut.security.filter.enabled" to security,
          "micronaut.server.port" to -1,
          "micronaut.metrics.enabled" to false,
          "micronaut.executors.scheduler.type" to "cached",
        ),
      ).build()
      .use { context ->
        val business = mutableListOf<Any>()
        for (parameter in controllers
          .flatMap {
            it.declaredConstructors
              .single { !it.isSynthetic }
              .parameters
              .toList()
          }.distinctBy {
            it.type to it.getAnnotation(Named::class.java)?.value
          }) {
          @Suppress("UNCHECKED_CAST")
          val type = parameter.type as Class<Any>
          val instance =
            when {
              type.name == "io.airbyte.server.wrapped.apis.ApiHelper" -> type.getConstructor().newInstance()
              else ->
                mockkClass(
                  type.kotlin,
                  relaxed =
                    type !in
                      setOf(
                        JobsHandler::class.java,
                        JobHistoryHandler::class.java,
                        JobInputHandler::class.java,
                        SchedulerHandler::class.java,
                        AttemptHandler::class.java,
                        JobConverter::class.java,
                        JobObservabilityService::class.java,
                        StreamResetRecordsHelper::class.java,
                        RetryStatesHandler::class.java,
                      ),
                )
            }
          val name = parameter.getAnnotation(Named::class.java)?.value
          configureDependency(instance)
          if (type.name != "io.airbyte.server.wrapped.apis.ApiHelper") {
            business += instance
            configureBusiness(instance)
          }
          if (name == null) context.registerSingleton(type, instance) else context.registerSingleton(type, instance, Qualifiers.byName(name))
        }
        val data = mockk<AuthorizationDataService>()
        every { data.replicationJobConnectionId(any()) } throws AuthorizationResourceNotFoundException()
        every { data.replicationJobConnectionId(7) } returns CONNECTION
        every { data.savedAttemptConnectionId(any(), any()) } throws AuthorizationResourceNotFoundException()
        every { data.savedAttemptConnectionId(7, 0) } returns CONNECTION
        if (dataFailure) every { data.replicationJobConnectionId(7) } throws java.io.IOException("job ownership unavailable")
        every { data.workspaceAndOrganization(WORKSPACE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(OTHER_WORKSPACE) } returns WorkspaceAndOrganization(OTHER_WORKSPACE, UUID(0, 13))
        every { data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
        every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        val grants = mockk<PermissionService>()
        val savedGrants =
          if (!granted) {
            emptyList()
          } else {
            listOf(
              Permission().withPermissionType(grant).also {
                if (grant.name.startsWith("WORKSPACE")) it.workspaceId = WORKSPACE
                if (grant.name.startsWith("ORGANIZATION")) it.organizationId = ORGANIZATION
              },
            )
          }
        every { grants.getPermissionsByAuthUserId("caller") } returns savedGrants
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns savedGrants
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
        for (type in controllers) {
          registerSiblingOperationRules(
            context,
            type,
            setOf(
              JobReadRule::class.java,
              JobExplainUnsupportedRule::class.java,
              JobReadOrWorkerRule::class.java,
              JobAdminRule::class.java,
              JobRunRule::class.java,
              JobCreateRule::class.java,
              JobInputRule::class.java,
              JobAdminReadRule::class.java,
            ),
          )
        }
        context.start()
        val route =
          context
            .getBean(Router::class.java)
            .uriRoutes()
            .filter {
              it.httpMethodName == "POST" && it.uriMatchTemplate.toString() == path
            }.findFirst()
            .orElseThrow()
        assertEquals(rule, route.annotationMetadata.classValue(AuthorizeOperation::class.java, "rule").orElseThrow(), path)
        business.forEach { clearMocks(it, answers = false, recordedCalls = true) }
        context.getBean(EmbeddedServer::class.java).start().use { server ->
          HttpClient.newHttpClient().use { client ->
            val response =
              client.send(
                java.net.http.HttpRequest
                  .newBuilder(URI("${server.url}$path"))
                  .header("Content-Type", "application/json")
                  .header("Authorization", "Bearer $token")
                  .POST(
                    java.net.http.HttpRequest.BodyPublishers
                      .ofString(body),
                  ).build(),
                HttpResponse.BodyHandlers.ofString(),
              )
            assertEquals(status, response.statusCode(), "$path $body: ${response.body()}")
            if (!cloud && path.endsWith("/explain")) for (dependency in business) verify { dependency wasNot Called }
            if (unusedOwner) verify { data wasNot Called }
            if (status !in setOf(200, 204, 501)) for (dependency in business) verify { dependency wasNot Called }
            if (status == 200 && responseBody) {
              org.junit.jupiter.api.Assertions
                .assertTrue(response.body().isNotBlank(), path)
            }
            if (!security) {
              verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
              verify(exactly = 0) { grants.getPermissionsByServiceAccountId(any()) }
            }
          }
        }
      }
  }

  private fun configureBusiness(instance: Any) {
    when (instance) {
      is JobsHandler -> {
        every { instance.jobFailure(match { it.jobId == 7L && it.connectionId == CONNECTION }) } returns InternalOperationResult(succeeded = true)
        every { instance.jobSuccessWithAttemptNumber(match { it.jobId == 7L && it.connectionId == CONNECTION && it.attemptNumber == 0 }) } returns
          InternalOperationResult(succeeded = true)
        every { instance.failNonTerminalJobs(CONNECTION) } just Runs
        every { instance.reportJobStart(7) } returns InternalOperationResult(succeeded = true)
        every { instance.setJobQueued(7) } returns InternalOperationResult(succeeded = true)
        every { instance.cancelQueuedJob(7) } returns InternalOperationResult(succeeded = true)
        every { instance.checkDataWorkerCapacity(7, CONNECTION, ORGANIZATION) } returns
          CheckDataWorkerCapacityRead(capacityAvailable = true, useOnDemandCapacity = false)
        every { instance.didPreviousJobSucceed(CONNECTION, 7) } returns BooleanRead(value = false)
        every { instance.persistJobCancellation(CONNECTION, 7, 0, any()) } just Runs
      }
      is JobHistoryHandler -> {
        every { instance.getJobInfo(7) } returns responseFixture()
        every { instance.getJobInfoWithoutLogs(7) } returns responseFixture()
        every { instance.getJobDebugInfo(7) } returns responseFixture()
        every { instance.getJobInfoLight(match { it.id == 7L }) } returns responseFixture()
        every { instance.getLastReplicationJob(match { it.connectionId == CONNECTION }) } returns responseFixture()
        every { instance.getLastReplicationJobWithCancel(match { it.connectionId == CONNECTION }) } returns responseFixture()
        every { instance.listJobsFor(match { it.configId == CONNECTION.toString() || it.configId == "opaque" }) } returns responseFixture()
        every { instance.listJobsForWorkspaces(match { it.workspaceIds?.all { id -> id == WORKSPACE } == true }) } returns responseFixture()
      }
      is JobInputHandler -> {
        every { instance.getJobInput(match { it.jobId == 7L && it.attemptNumber == 0 }) } returns emptyMap<String, Any>()
        every { instance.getJobWebhookConfig(7) } returns
          JobWebhookConfig().withOperationSequence(emptyList()).withWebhookOperationConfigs(Jsons.emptyObject())
      }
      is SchedulerHandler -> {
        every { instance.cancelJob(match { it.id == 7L }) } returns responseFixture()
        every { instance.createJob(match { it.connectionId == CONNECTION }) } returns responseFixture()
      }
      is AttemptHandler -> {
        every { instance.createNewAttemptNumber(7) } returns responseFixture()
        every { instance.getAttemptCombinedStats(7, 0) } returns responseFixture()
        every { instance.getAttemptForJob(7, 0) } returns mockk()
        every { instance.saveSyncConfig(match { it.jobId == 7L && it.attemptNumber == 0 }) } returns InternalOperationResult(succeeded = true)
        every { instance.failAttempt(0, 7, any(), any()) } just Runs
      }
      is JobConverter -> every { instance.getAttemptInfoRead(any()) } returns responseFixture()
      is JobObservabilityService -> {
        every { instance.evaluateOutlier(7) } just Runs
        every { instance.finalizeStats(7) } just Runs
      }
      is StreamResetRecordsHelper -> every { instance.deleteStreamResetRecordsForJob(7, CONNECTION) } just Runs
      is RetryStatesHandler -> {
        every { instance.getByJobId(match { it.id == 7L }) } returns responseFixture()
        every { instance.putByJobId(match { it.jobId == 7L && it.connectionId == CONNECTION }) } just Runs
      }
    }
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: Class<*>,
    val grant: Permission.PermissionType,
    val status: Int = 200,
    val responseBody: Boolean = true,
  ) {
    val token: String get() =
      if (path in
        WORKER_PATHS
      ) {
        "service"
      } else {
        "valid"
      }
  }

  private fun scenarios(): List<Scenario> {
    val id = """{"id":7}"""
    val job = """{"jobId":7}"""
    val connection = """{"connectionId":"$CONNECTION"}"""
    val pair = """{"jobId":7,"attemptNumber":0}"""
    val connectedJob = """{"jobId":7,"connectionId":"$CONNECTION"}"""
    val reader = Permission.PermissionType.WORKSPACE_READER
    val admin = Permission.PermissionType.INSTANCE_ADMIN
    return listOf(
      Scenario("/api/v1/jobs/cancel", id, JobRunRule::class.java, Permission.PermissionType.WORKSPACE_RUNNER),
      Scenario(
        "/api/v1/jobs/create",
        """{"connectionId":"$CONNECTION","isScheduled":false}""",
        JobCreateRule::class.java,
        Permission.PermissionType.WORKSPACE_RUNNER,
      ),
      Scenario("/api/v1/jobs/cancelQueuedJob", job, JobAdminRule::class.java, admin),
      Scenario(
        "/api/v1/jobs/checkDataWorkerCapacity",
        """{"jobId":7,"connectionId":"$CONNECTION","organizationId":"$ORGANIZATION"}""",
        JobAdminRule::class.java,
        admin,
      ),
      Scenario("/api/v1/jobs/delete_stream_reset_records", connectedJob, JobAdminRule::class.java, admin, 200, false),
      Scenario("/api/v1/jobs/did_previous_job_succeed", connectedJob, JobAdminReadRule::class.java, admin),
      Scenario("/api/v1/jobs/evaluate_outlier", id, JobAdminRule::class.java, admin),
      Scenario("/api/v1/jobs/explain", id, if (cloud) JobReadRule::class.java else JobExplainUnsupportedRule::class.java, reader),
      Scenario("/api/v1/jobs/fail_non_terminal", connection, JobAdminRule::class.java, admin, 200, false),
      Scenario("/api/v1/jobs/finalize", id, JobAdminRule::class.java, admin),
      Scenario("/api/v1/jobs/get", id, JobReadRule::class.java, reader),
      Scenario("/api/v1/jobs/getWebhookConfig", job, JobReadRule::class.java, reader),
      Scenario("/api/v1/jobs/get_debug_info", id, JobReadRule::class.java, reader),
      Scenario("/api/v1/jobs/get_input", pair, JobInputRule::class.java, reader),
      Scenario("/api/v1/jobs/get_last_replication_job", connection, JobReadOrWorkerRule::class.java, reader),
      Scenario("/api/v1/jobs/get_last_replication_job_with_cancel", connection, JobReadRule::class.java, reader),
      Scenario("/api/v1/jobs/get_light", id, JobReadRule::class.java, reader),
      Scenario("/api/v1/jobs/get_without_logs", id, JobReadRule::class.java, reader),
      Scenario(
        "/api/v1/jobs/job_failure",
        """{"jobId":7,"connectionId":"$CONNECTION","attemptNumber":0,"reason":"test"}""",
        JobAdminRule::class.java,
        admin,
      ),
      Scenario(
        "/api/v1/jobs/job_success_with_attempt_number",
        """{"jobId":7,"connectionId":"$CONNECTION","attemptNumber":0}""",
        JobAdminRule::class.java,
        admin,
      ),
      Scenario("/api/v1/jobs/list", """{"configTypes":["sync"],"configId":"$CONNECTION"}""", JobReadRule::class.java, reader),
      Scenario("/api/v1/jobs/list_for_workspaces", """{"configTypes":["sync"],"workspaceIds":["$WORKSPACE"]}""", JobReadRule::class.java, reader),
      Scenario(
        "/api/v1/jobs/persist_cancellation",
        """{"jobId":7,"connectionId":"$CONNECTION","attemptNumber":0,"attemptFailureSummary":{}}""",
        JobAdminRule::class.java,
        admin,
        200,
        false,
      ),
      Scenario("/api/v1/jobs/reportJobStart", connectedJob, JobAdminRule::class.java, admin),
      Scenario(
        "/api/v1/jobs/retry_states/create_or_update",
        """{"jobId":7,"connectionId":"$CONNECTION","successiveCompleteFailures":0,"totalCompleteFailures":0,"successivePartialFailures":0,"totalPartialFailures":0}""",
        JobAdminRule::class.java,
        admin,
        204,
      ),
      Scenario("/api/v1/jobs/retry_states/get", id, JobAdminReadRule::class.java, admin),
      Scenario("/api/v1/jobs/setJobQueued", job, JobAdminRule::class.java, admin),
      Scenario("/api/v1/attempt/create_new_attempt_number", job, JobAdminRule::class.java, admin),
      Scenario("/api/v1/attempt/fail", pair, JobAdminRule::class.java, admin, 200, false),
      Scenario("/api/v1/attempt/get_combined_stats", pair, JobReadRule::class.java, reader),
      Scenario("/api/v1/attempt/get_for_job", pair, JobReadRule::class.java, reader),
      Scenario(
        "/api/v1/attempt/save_sync_config",
        """{"jobId":7,"attemptNumber":0,"syncConfig":{"sourceConfiguration":{},"destinationConfiguration":{}}}""",
        JobAdminRule::class.java,
        admin,
      ),
    )
  }

  companion object {
    val WORKER_PATHS =
      setOf(
        "/api/v1/jobs/cancelQueuedJob",
        "/api/v1/jobs/checkDataWorkerCapacity",
        "/api/v1/jobs/delete_stream_reset_records",
        "/api/v1/jobs/create",
        "/api/v1/jobs/get_input",
        "/api/v1/jobs/evaluate_outlier",
        "/api/v1/jobs/fail_non_terminal",
        "/api/v1/jobs/finalize",
        "/api/v1/jobs/job_failure",
        "/api/v1/jobs/job_success_with_attempt_number",
        "/api/v1/jobs/persist_cancellation",
        "/api/v1/jobs/reportJobStart",
        "/api/v1/jobs/retry_states/create_or_update",
        "/api/v1/jobs/setJobQueued",
        "/api/v1/attempt/create_new_attempt_number",
        "/api/v1/attempt/fail",
        "/api/v1/attempt/save_sync_config",
      )
    val CONNECTION: UUID = UUID(0, 1)
    val OTHER_CONNECTION: UUID = UUID(0, 2)
    val WORKSPACE: UUID = UUID(0, 3)
    val OTHER_WORKSPACE: UUID = UUID(0, 12)
    val ORGANIZATION: UUID = UUID(0, 4)
    val SOURCE: UUID = UUID(0, 5)
    val DESTINATION: UUID = UUID(0, 6)
  }
}

@Singleton
@Requires(property = "spec.name", value = "RemainingJobsAuthorizationHttpTest")
class RemainingJobsAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val token =
      request.headers.authorization
        .orElse("")
        .removePrefix("Bearer ")
    val attributes =
      when (token) {
        "valid", "application-owner" -> emptyMap()
        "internal" -> mapOf(TokenType.INTERNAL_CLIENT.toClaim())
        "service" -> mapOf(TokenType.SERVICE_ACCOUNT.toClaim())
        "dataplane" -> mapOf(TokenType.DATAPLANE_V1.toClaim())
        "service-other-scope" -> mapOf(TokenType.SERVICE_ACCOUNT.toClaim(), TokenScopeClaim.CLAIM_ID to TokenScopeClaim(UUID(0, 99).toString()))
        "workload" -> mapOf(TokenType.WORKLOAD_API.toClaim())
        "embedded" -> mapOf(TokenType.EMBEDDED_V1.toClaim())
        "other-scope" -> mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(UUID(0, 99).toString()))
        else -> return Publishers.empty()
      }
    return Publishers.just(
      Authentication.build(
        if (token in
          setOf("service", "dataplane", "service-other-scope")
        ) {
          UUID(0, 9).toString()
        } else {
          "caller"
        },
        listOf(AuthRoleConstants.ADMIN),
        attributes,
      ),
    )
  }
}

@Singleton
@Requires(property = "spec.name", value = "RemainingJobsAuthorizationHttpTest")
class RemainingJobsEarlierAllow : SecurityRule<HttpRequest<*>> {
  override fun getOrder(): Int = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> = Publishers.just(SecurityRuleResult.ALLOWED)
}
