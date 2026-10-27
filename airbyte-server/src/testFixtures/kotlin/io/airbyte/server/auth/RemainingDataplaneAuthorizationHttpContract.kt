/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Permission
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.shared.DataplaneWithServiceAccount
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.ServiceAccount
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.DataplaneAuthorizationDataService
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.server.apis.controllers.DataWorkerAllocationApiController
import io.airbyte.server.apis.controllers.DataplaneController
import io.airbyte.server.apis.controllers.DataplaneGroupApiController
import io.airbyte.server.services.DataplaneService
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingDataplaneAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `organization admin cannot create globally usable dataplane credentials`() {
    exercise(
      Scenario(
        "/api/v1/dataplanes/create",
        """{"dataplane_group_id":"$GROUP","name":"fixture","enabled":true,"instanceScope":true}""",
        "io.airbyte.server.auth.DataplaneCreateRule",
      ),
      403,
      checkDeclaration = false,
    )
  }

  @Test
  fun `global dataplane credentials require instance admin while ordinary creation keeps organization administration`() {
    val create = scenarios().single { it.path == "/api/v1/dataplanes/create" }
    exercise(create, 200)
    val global = create.copy(body = create.body.replace("false", "true"))
    exercise(global, 403)
    exercise(global, 200, grantOverride = Permission.PermissionType.INSTANCE_ADMIN)
    exercise(global, 403, token = "scoped:${UUID(0, 1)}", grantOverride = Permission.PermissionType.INSTANCE_ADMIN)
    exercise(global, 404, missing = true, grantOverride = Permission.PermissionType.INSTANCE_ADMIN)
    exercise(global, 500, lookupError = true, grantOverride = Permission.PermissionType.INSTANCE_ADMIN)
    exercise(global, 200, token = "internal")
    exercise(global, 200, token = "service", grantOverride = Permission.PermissionType.INSTANCE_ADMIN)
    exercise(global, 403, token = "service")
    exercise(global, 200, granted = false, security = false)
  }

  @Test
  fun `dataplane management and machine identity routes reject unsupported callers and another service account before business`() {
    val cases =
      listOf(
        arrayOf("/api/v1/dataplane_group/create", """{"organization_id":"$ORGANIZATION","name":"fixture","enabled":true}""", "workload"),
        arrayOf("/api/v1/dataplanes/create", """{"dataplane_group_id":"$GROUP","name":"fixture","enabled":true,"instanceScope":false}""", "workload"),
        arrayOf("/api/v1/dataplanes/initialize", """{"client_id":"$SERVICE_ACCOUNT"}""", "user"),
        arrayOf("/api/v1/dataplanes/heartbeat", """{"client_id":"$OTHER_SERVICE_ACCOUNT"}""", "service"),
        arrayOf(
          "/api/v1/data_worker_allocation/reallocate",
          """{"organization_id":"$ORGANIZATION","from_dataplane_group_id":"$SOURCE_GROUP","to_dataplane_group_id":"$DEFAULT_GROUP","amount":1}""",
          "workload",
        ),
      )
    assertAll(
      cases.map { (path, body, token) ->
        Executable {
          RemainingOperationHttpSupport(cloud, controllers(), rules()).run(
            configure = { context, business ->
              configureBusiness(business)
              configureAuthorization(context, true)
            },
            check = { _, server, business ->
              val response = RemainingOperationHttpSupport.request(server, "POST", path, body, token)
              assertEquals(403, response.statusCode(), "$path: ${response.body()}")
              RemainingOperationHttpSupport.requireNoBusiness(business)
            },
          )
        }
      },
    )
  }

  @Test
  fun `all migrated dataplane operations execute their independent declared actions and real responses`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, status(scenario)) } })
  }

  @Test
  fun `all migrated dataplane operations deny before business and preserve insufficient action boundaries`() {
    assertAll(
      scenarios().flatMap { scenario ->
        listOf(
          Executable { exercise(scenario, 403, granted = false) },
          Executable { exercise(scenario, 403, token = "workload") },
          Executable { exercise(scenario, 403, grantOverride = Permission.PermissionType.ORGANIZATION_READER) },
        ).let { if (scenario.rule.endsWith("DataplaneGroupListRule")) it.drop(1).take(1) else it }
      },
    )
  }

  @Test
  fun `machine operations require matching supported token identity saved assignment and actual dataplane permission`() {
    for (scenario in scenarios().filter { it.machine }) {
      for (token in listOf("user", "application-owner", "embedded", "workload", "scoped:${UUID(0, 1)}")) exercise(scenario, 403, token = token)
      for (token in listOf("service", "dataplane", "internal")) exercise(scenario, status(scenario), token = token)
      exercise(scenario.copy(body = """{"client_id":"$OTHER_SERVICE_ACCOUNT"}"""), 403)
      exercise(scenario, 500, assignmentError = true)
    }
  }

  @Test
  fun `saved organization errors propagate with zero business and configured no auth preserves operations`() {
    for (scenario in scenarios()) {
      exercise(scenario, if (!cloud && scenario.cloudOnly) 501 else 404, missing = true)
      exercise(scenario, if (!cloud && scenario.cloudOnly) 501 else 500, lookupError = true)
      exercise(scenario, 403, token = "absent")
      exercise(scenario, status(scenario), granted = false, security = false)
    }
  }

  @Test
  fun `OSS allocation stubs retain organization or instance admin without tenant and region reads`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.cloudOnly }.map { scenario ->
        Executable {
          exercise(scenario, 501, missing = true, checkDeclaration = false, unusedOwner = true)
          exercise(scenario, 501, lookupError = true, regionError = true, checkDeclaration = false, unusedOwner = true)
          exercise(scenario, 501, missing = true, security = false, granted = false, checkDeclaration = false, unusedOwner = true)
          exercise(scenario, 403, granted = false, checkDeclaration = false, unusedOwner = true)
          exercise(scenario, 403, token = "workload", checkDeclaration = false, unusedOwner = true)
          exercise(scenario, 403, token = "scoped:${UUID(0, 1)}", checkDeclaration = false, unusedOwner = true)
        }
      },
    )
  }

  @Test
  fun `OSS allocation stubs reject omitted and null selectors before business`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.cloudOnly }.flatMap { scenario ->
        listOf(
          "{}",
          """{"organization_id":null,"dataplane_group_id":null,"from_dataplane_group_id":null,"to_dataplane_group_id":null}""",
        ).flatMap { body ->
          val request = scenario.copy(body = body)
          listOf(
            Executable { exercise(request, 400, security = false, granted = false, unusedOwner = true) },
            Executable { exercise(request, 400, grantOverride = Permission.PermissionType.INSTANCE_ADMIN, unusedOwner = true) },
            Executable { exercise(request, 400, granted = false, unusedOwner = true) },
            Executable { exercise(request, 400, grantOverride = Permission.PermissionType.ORGANIZATION_READER, unusedOwner = true) },
            Executable { exercise(request, 400, token = "workload", grantOverride = Permission.PermissionType.INSTANCE_ADMIN, unusedOwner = true) },
            Executable {
              exercise(
                request,
                400,
                token = "scoped:${UUID(0, 1)}",
                grantOverride = Permission.PermissionType.INSTANCE_ADMIN,
                unusedOwner = true,
              )
            },
          )
        }
      },
    )
  }

  @Test
  fun `allocation reads keep unknown region zero and mutations validate only their consumed region branches`() {
    val get = scenarios().single { it.path.endsWith("data_worker_allocation/get") }
    exercise(get.copy(body = """{"organization_id":"$ORGANIZATION","dataplane_group_id":"${UUID(0, 99)}"}"""), status(get), regionError = true)
    if (cloud) {
      for (scenario in scenarios().filter { it.rule.endsWith("ReallocateRule") || it.rule.endsWith("AddRule") || it.rule.endsWith("RemoveRule") }) {
        exercise(
          scenario,
          400,
          regionError = true,
        )
      }
      val add = scenarios().single { it.path.endsWith("add_capacity") }
      exercise(add.copy(body = """{"organization_id":"$ORGANIZATION","amount":1}"""), 200)
      exercise(add.copy(body = """{"organization_id":"$ORGANIZATION","amount":1}"""), 500, defaultError = true)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = if (scenario.machine) "service" else "user",
    granted: Boolean = true,
    grantOverride: Permission.PermissionType = scenario.grant,
    missing: Boolean = false,
    lookupError: Boolean = false,
    assignmentError: Boolean = false,
    regionError: Boolean = false,
    defaultError: Boolean = false,
    security: Boolean = true,
    checkDeclaration: Boolean = true,
    unusedOwner: Boolean = false,
  ) {
    RemainingOperationHttpSupport(cloud, controllers(), rules(), security).run(
      configure = { context, business ->
        configureBusiness(business)
        configureAuthorization(context, granted, grantOverride, missing, lookupError, assignmentError, regionError, defaultError, security)
      },
      check = { context, server, business ->
        if (checkDeclaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, scenario.rule)
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected !in setOf(200, 501)) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200) assertTrue(response.body().isNotBlank())
        if (!cloud && scenario.cloudOnly) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (unusedOwner) {
          val data = context.getBean(AuthorizationDataService::class.java)
          val assignments = context.getBean(DataplaneAuthorizationDataService::class.java)
          verify { data wasNot Called }
          verify { assignments wasNot Called }
        }
      },
    )
  }

  private fun configureAuthorization(
    context: io.micronaut.context.ApplicationContext,
    granted: Boolean,
    grant: Permission.PermissionType = Permission.PermissionType.INSTANCE_ADMIN,
    missing: Boolean = false,
    lookupError: Boolean = false,
    assignmentError: Boolean = false,
    regionError: Boolean = false,
    defaultError: Boolean = false,
    security: Boolean = true,
  ) {
    val data = mockk<AuthorizationDataService>()
    every { data.organization(ORGANIZATION) } returns ORGANIZATION
    every { data.dataplaneOrganization(DATAPLANE) } answers { data.organization(ORGANIZATION) }
    every { data.dataplaneGroupOrganization(GROUP) } answers { data.organization(ORGANIZATION) }
    if (missing) every { data.organization(ORGANIZATION) } throws AuthorizationResourceNotFoundException()
    if (lookupError) every { data.organization(ORGANIZATION) } throws java.io.IOException("organization unavailable")
    val assignments = mockk<DataplaneAuthorizationDataService>()
    every { assignments.clientOrganization(SERVICE_ACCOUNT.toString()) } answers { data.organization(ORGANIZATION) }
    every { assignments.clientOrganization(OTHER_SERVICE_ACCOUNT.toString()) } answers { data.organization(ORGANIZATION) }
    if (assignmentError) every { assignments.clientOrganization(any()) } throws java.io.IOException("saved assignment unavailable")
    every { assignments.requireUsableRegion(ORGANIZATION, DEFAULT_GROUP) } just Runs
    every { assignments.requireRegion(SOURCE_GROUP, false) } just Runs
    every { assignments.requireRegion(DEFAULT_GROUP, true) } just Runs
    every { assignments.requireDefaultRegion() } just Runs
    if (regionError) {
      every { assignments.requireUsableRegion(any(), any()) } throws
        io.airbyte.api.problems.throwable.generated
          .BadRequestProblem()
      every { assignments.requireRegion(any(), any()) } throws
        io.airbyte.api.problems.throwable.generated
          .BadRequestProblem()
    }
    if (defaultError) every { assignments.requireDefaultRegion() } throws IllegalStateException("default region missing")
    val permissions = mockk<PermissionService>()
    val permission = Permission().withPermissionType(grant)
    if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.organizationId = ORGANIZATION
    every { permissions.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
    every { permissions.getPermissionsByServiceAccountId(SERVICE_ACCOUNT) } returns if (granted) listOf(permission) else emptyList()
    context.registerSingleton(AuthorizationDataService::class.java, data)
    context.registerSingleton(DataplaneAuthorizationDataService::class.java, assignments)
    context.registerSingleton(
      OperationPermissions::class.java,
      OperationPermissions(
        permissions,
        RoleResolver(
          mockk(),
          mockk(),
          mockk(),
          mockk(),
        ),
        security,
      ),
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val grant: Permission.PermissionType = Permission.PermissionType.ORGANIZATION_ADMIN,
    val machine: Boolean = false,
    val cloudOnly: Boolean = false,
  )

  private fun status(scenario: Scenario) = if (scenario.cloudOnly && !cloud) 501 else 200

  private fun rules() = scenarios().map { it.rule }.toSet()

  private fun scenarios(): List<Scenario> =
    listOf(
      Scenario(
        "/api/v1/dataplanes/create",
        """{"dataplane_group_id":"$GROUP","name":"fixture","enabled":true,"instanceScope":false}""",
        "io.airbyte.server.auth.DataplaneCreateRule",
      ),
      Scenario(
        "/api/v1/dataplane_group/create",
        """{"organization_id":"$ORGANIZATION","name":"fixture","enabled":true}""",
        "io.airbyte.server.auth.DataplaneGroupManageRule",
      ),
      Scenario(
        "/api/v1/dataplane_group/update",
        """{"dataplane_group_id":"$GROUP","name":"fixture","enabled":true}""",
        "io.airbyte.server.auth.DataplaneGroupManageRule",
      ),
      Scenario("/api/v1/dataplane_group/delete", """{"dataplane_group_id":"$GROUP"}""", "io.airbyte.server.auth.DataplaneGroupManageRule"),
      Scenario("/api/v1/dataplane_group/list", """{"organization_id":"$ORGANIZATION"}""", "io.airbyte.server.auth.DataplaneGroupListRule"),
      Scenario(
        "/api/v1/dataplanes/update",
        """{"dataplane_id":"$DATAPLANE","name":"fixture","enabled":true}""",
        "io.airbyte.server.auth.DataplaneManageRule",
      ),
      Scenario("/api/v1/dataplanes/delete", """{"dataplane_id":"$DATAPLANE"}""", "io.airbyte.server.auth.DataplaneManageRule"),
      Scenario("/api/v1/dataplanes/list", """{"dataplane_group_id":"$GROUP"}""", "io.airbyte.server.auth.DataplaneManageRule"),
      Scenario("/api/v1/dataplanes/health", """{"organizationId":"$ORGANIZATION"}""", "io.airbyte.server.auth.DataplaneManageRule"),
      Scenario(
        "/api/v1/dataplanes/initialize",
        """{"client_id":"$SERVICE_ACCOUNT"}""",
        "io.airbyte.server.auth.DataplaneMachineRule",
        Permission.PermissionType.DATAPLANE,
        machine = true,
      ),
      Scenario(
        "/api/v1/dataplanes/heartbeat",
        """{"client_id":"$SERVICE_ACCOUNT"}""",
        "io.airbyte.server.auth.DataplaneMachineRule",
        Permission.PermissionType.DATAPLANE,
        machine = true,
      ),
      Scenario(
        "/api/v1/data_worker_allocation/list",
        """{"organization_id":"$ORGANIZATION"}""",
        if (cloud) {
          "io.airbyte.server.auth.DataWorkerAllocationOrganizationRule"
        } else {
          "io.airbyte.server.auth.DataWorkerAllocationOrganizationUnsupportedRule"
        },
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/data_worker_allocation/get",
        """{"organization_id":"$ORGANIZATION","dataplane_group_id":"$DEFAULT_GROUP"}""",
        if (cloud) {
          "io.airbyte.server.auth.DataWorkerAllocationOrganizationRule"
        } else {
          "io.airbyte.server.auth.DataWorkerAllocationOrganizationUnsupportedRule"
        },
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/data_worker_allocation/reallocate",
        """{"organization_id":"$ORGANIZATION","from_dataplane_group_id":"$SOURCE_GROUP","to_dataplane_group_id":"$DEFAULT_GROUP","amount":1}""",
        if (cloud) {
          "io.airbyte.server.wrapped.auth.DataWorkerAllocationReallocateRule"
        } else {
          "io.airbyte.server.auth.DataWorkerAllocationOrganizationUnsupportedRule"
        },
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/data_worker_allocation/add_capacity",
        """{"organization_id":"$ORGANIZATION","dataplane_group_id":"$DEFAULT_GROUP","amount":1}""",
        if (cloud) {
          "io.airbyte.server.wrapped.auth.DataWorkerAllocationAddRule"
        } else {
          "io.airbyte.server.auth.OssInstanceAdminUnsupportedRule"
        },
        Permission.PermissionType.INSTANCE_ADMIN,
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/data_worker_allocation/remove_capacity",
        """{"organization_id":"$ORGANIZATION","dataplane_group_id":"$SOURCE_GROUP","amount":1}""",
        if (cloud) {
          "io.airbyte.server.wrapped.auth.DataWorkerAllocationRemoveRule"
        } else {
          "io.airbyte.server.auth.OssInstanceAdminUnsupportedRule"
        },
        Permission.PermissionType.INSTANCE_ADMIN,
        cloudOnly = true,
      ),
    )

  private fun configureBusiness(business: List<Any>) {
    business.forEach { instance ->
      when (instance) {
        is DataplaneService -> {
          every { instance.listDataplanes(GROUP) } returns emptyList()
          every { instance.listDataplanes(DEFAULT_GROUP) } returns emptyList()
          every { instance.listDataplanes(listOf(GROUP)) } returns listOf(dataplane())
          every {
            instance.createNewDataplane(
              match { it.dataplaneGroupId == GROUP && it.name == "fixture" },
              false,
            )
          } returns
            DataplaneWithServiceAccount(
              dataplane(),
              ServiceAccount(SERVICE_ACCOUNT, "fixture", "fixture-secret"),
            )
          every {
            instance.createNewDataplane(
              match { it.dataplaneGroupId == GROUP && it.name == "fixture" },
              true,
            )
          } returns
            DataplaneWithServiceAccount(
              dataplane(),
              ServiceAccount(SERVICE_ACCOUNT, "fixture", "fixture-secret"),
            )
          every { instance.getDataplaneByServiceAccountId(SERVICE_ACCOUNT.toString()) } returns dataplane()
          every { instance.getDataplaneByServiceAccountId(OTHER_SERVICE_ACCOUNT.toString()) } returns
            dataplane()
              .withId(
                UUID(
                  0,
                  13,
                ),
              ).withServiceAccountId(OTHER_SERVICE_ACCOUNT)
          every { instance.getDataplane(DATAPLANE.toString()) } returns dataplane()
          every { instance.updateDataplane(DATAPLANE, any(), any()) } returns dataplane()
          every { instance.deleteDataplane(DATAPLANE) } returns dataplane().withTombstone(true)
        }
        is DataplaneGroupService -> {
          every { instance.getDataplaneGroup(GROUP) } returns group()
          every { instance.getOrganizationIdFromDataplaneGroup(GROUP) } returns ORGANIZATION
          every { instance.writeDataplaneGroup(match { it.organizationId == ORGANIZATION }) } returns group()
          every {
            instance.listDataplaneGroups(
              listOf(DEFAULT_ORGANIZATION_ID),
              false,
            )
          } returns listOf(group().withId(DEFAULT_GROUP).withOrganizationId(DEFAULT_ORGANIZATION_ID))
          every {
            instance.listDataplaneGroups(
              listOf(DEFAULT_ORGANIZATION_ID),
              true,
            )
          } returns listOf(group().withId(DEFAULT_GROUP).withOrganizationId(DEFAULT_ORGANIZATION_ID))
          every { instance.listDataplaneGroups(listOf(ORGANIZATION), false) } returns listOf(group())
        }
        is RoleResolver -> {
          val request = mockk<RoleResolver.Request>()
          every { instance.newRequest() } returns request
          every { request.withCurrentAuthentication() } returns request
          every { request.withOrg(ORGANIZATION) } returns request
          every { request.requireRole(any()) } just Runs
        }
        is EntitlementService -> {
          every { instance.ensureEntitled(OrganizationId(ORGANIZATION), any()) } just Runs
          every {
            instance.checkEntitlement(
              OrganizationId(ORGANIZATION),
              PrivateLinkEntitlement,
            )
          } returns
            EntitlementResult(
              PrivateLinkEntitlement.featureId,
              false,
            )
          every {
            instance.checkEntitlement(
              OrganizationId(ORGANIZATION),
              SelfManagedRegionsEntitlement,
            )
          } returns
            EntitlementResult(
              SelfManagedRegionsEntitlement.featureId,
              false,
            )
        }
        is MetricClient -> every { instance.count(any(), any(), *anyVararg()) } returns null
        is AirbyteConfig -> every { instance.version } returns "fixture"
        is DataplaneHealthService -> {
          every { instance.recordHeartbeat(any(), "fixture", null) } just Runs
          every { instance.getDataplaneHealthInfos(listOf(DATAPLANE)) } returns emptyList()
        }
      }
      configureDependency(instance)
    }
  }

  private fun controllers() =
    listOf(
      DataplaneController::class.java,
      DataplaneGroupApiController::class.java,
      if (cloud) {
        Class.forName(
          "io.airbyte.server.wrapped.apis.controllers.DataWorkerAllocationWrappedController",
        )
      } else {
        DataWorkerAllocationApiController::class.java
      },
    )

  companion object {
    val ORGANIZATION: UUID = UUID(0, 2)
    val GROUP: UUID = UUID(0, 3)
    val DATAPLANE: UUID = UUID(0, 4)
    val SERVICE_ACCOUNT: UUID = UUID(0, 9)
    val OTHER_SERVICE_ACCOUNT: UUID = UUID(0, 19)
    val DEFAULT_GROUP: UUID = UUID(0, 31)
    val SOURCE_GROUP: UUID = UUID(0, 32)

    fun group(): DataplaneGroup =
      DataplaneGroup()
        .withId(
          GROUP,
        ).withOrganizationId(ORGANIZATION)
        .withName("fixture")
        .withEnabled(true)
        .withTombstone(false)
        .withCreatedAt(0)
        .withUpdatedAt(0)

    fun dataplane(): Dataplane =
      Dataplane()
        .withId(
          DATAPLANE,
        ).withDataplaneGroupId(
          GROUP,
        ).withServiceAccountId(SERVICE_ACCOUNT)
        .withName("fixture")
        .withEnabled(true)
        .withTombstone(false)
        .withCreatedAt(0)
        .withUpdatedAt(0)
  }
}
