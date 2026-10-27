/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.SecretPersistenceConfig
import io.airbyte.api.server.generated.models.SecretPersistenceType
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.SecretPersistenceConfigHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.secrets.SecretMigrationService
import io.airbyte.domain.services.secrets.SecretStorageCredentialsService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.server.apis.controllers.SecretStorageApiController
import io.airbyte.server.apis.controllers.SecretsPersistenceConfigApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.Optional
import java.util.UUID
import io.airbyte.api.server.generated.models.ScopeType as ApiScopeType

abstract class RemainingSecretsAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `secret storage and persistence APIs reject unsupported callers before hydration or secret writes`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `secret migration checks both stores organization before actor or persistence credential work`() {
    exercise(scenarios().single { it.migrate }, 500, foreignStore = true, declaration = false)
  }

  @Test
  fun `all seven secret operations declare exact admin or dataplane policy and execute genuine typed responses`() {
    for (scenario in scenarios()) {
      exercise(scenario, 200)
      exercise(scenario, 403, granted = false)
      exercise(scenario, 403, grant = Permission.PermissionType.ORGANIZATION_ADMIN)
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      exercise(scenario, 200, granted = false, security = false)
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
      if (scenario.read) exercise(scenario, 200, token = "service", grant = Permission.PermissionType.DATAPLANE)
    }
  }

  @Test
  fun `secret migration preserves explicit default source same organization and forbidden default destination branches`() {
    val migration = scenarios().single { it.migrate }
    exercise(migration.copy(body = migration.body.replace("\"fromSecretStorageId\":\"$STORAGE\"", "\"fromSecretStorageId\":\"${UUID(0, 0)}\"")), 200)
    exercise(
      migration.copy(
        body =
          migration.body.replace(
            "\"toSecretStorageId\":\"$SECOND_STORAGE\"",
            "\"toSecretStorageId\":\"${UUID(0, 0)}\"",
          ),
      ),
      500,
    )
    exercise(scenarios().single { it.path.endsWith("secret_storage/delete") }.copy(body = """{"secretStorageId":"${UUID(0, 0)}"}"""), 500)
    exercise(
      scenarios().single { it.path.endsWith("secrets_persistence_config/get") }.copy(body = """{"scopeType":"workspace","scopeId":"$WORKSPACE"}"""),
      422,
    )
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.INSTANCE_ADMIN,
    granted: Boolean = true,
    foreignStore: Boolean = false,
    security: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val storage =
      SecretStorage(
        SecretStorageId(STORAGE),
        SecretStorageScopeType.WORKSPACE,
        WORKSPACE,
        "fixture",
        SecretStorageType.LOCAL_TESTING,
        false,
        tombstone = true,
        createdBy = USER,
        updatedBy = USER,
        createdAt = null,
        updatedAt = null,
      )
    val mapper = MoreMappers.initMapper()
    val raw =
      io.airbyte.config
        .SecretPersistenceConfig()
        .withScopeType(
          ScopeType.ORGANIZATION,
        ).withScopeId(
          ORGANIZATION,
        ).withSecretPersistenceType(io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType.TESTING)
        .withConfiguration(emptyMap())
    RemainingOperationHttpSupport(
      cloud,
      listOf(
        SecretStorageApiController::class.java,
        SecretsPersistenceConfigApiController::class.java,
      ),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet(),
      security,
    ).run(
      configure = { context, business ->
        for (instance in business) {
          when (instance) {
            is CurrentUserService -> every { instance.getCurrentUser() } returns AuthenticatedUser().withUserId(USER).withAuthUserId("caller")
            is RoleResolver -> every { instance.newRequest() } returns mockk(relaxed = true)
            is SecretStorageService -> {
              every { instance.getById(SecretStorageId(STORAGE)) } returns storage
              every { instance.hydrateStorageConfig(storage) } returns
                SecretStorageWithConfig(storage, mapper.readTree("""{"fixture":"credential"}"""))
              every {
                instance.createSecretStorage(
                  match { it.scopeId == WORKSPACE && it.createdBy == UserId(USER) },
                  null,
                )
              } returns storage.copy(tombstone = false, configuredFromEnvironment = true)
              every { instance.deleteSecretStorage(SecretStorageId(STORAGE), UserId(USER)) } returns storage
              every { instance.listSecretStorage(SecretStorageScopeType.WORKSPACE, WORKSPACE) } returns listOf(storage.copy(tombstone = false))
            }
            is SecretStorageCredentialsService ->
              every {
                instance.writeStorageCredentials(
                  any(),
                  null,
                  SecretStorageId(STORAGE),
                  UserId(USER),
                )
              } just Runs
            is SecretMigrationService -> {
              every { instance.migrateSecrets(SecretStorageId(STORAGE), SecretStorageId(SECOND_STORAGE), ScopeType.WORKSPACE, WORKSPACE) } just Runs
              every {
                instance.migrateSecrets(
                  SecretStorage.DEFAULT_SECRET_STORAGE_ID,
                  SecretStorageId(SECOND_STORAGE),
                  ScopeType.WORKSPACE,
                  WORKSPACE,
                )
              } just Runs
            }
            is FeatureFlagClient -> every { instance.boolVariation(any(), any()) } returns true
            is SecretPersistenceConfigService -> {
              every { instance.get(ScopeType.ORGANIZATION, ORGANIZATION) } returns raw
              every {
                instance.createOrUpdate(
                  ScopeType.ORGANIZATION,
                  ORGANIZATION,
                  io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType.TESTING,
                  "fixture-coordinate",
                )
              } returns Optional.empty()
            }
            is SecretPersistenceConfigHandler -> {
              val coordinate = mockk<AirbyteManagedSecretCoordinate>()
              every { instance.buildRsmCoordinate(ScopeType.ORGANIZATION, ORGANIZATION) } returns coordinate
              every { instance.writeToEnvironmentSecretPersistence(coordinate, "{}") } returns "fixture-coordinate"
              every { instance.buildSecretPersistenceConfigResponse(raw) } returns
                SecretPersistenceConfig(
                  scopeType = ApiScopeType.ORGANIZATION,
                  scopeId = ORGANIZATION,
                  secretPersistenceType = SecretPersistenceType.TESTING,
                  configuration = mapper.readTree("{}"),
                )
            }
          }
        }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        every { data.secretStorageTarget(STORAGE) } returns AuthorizationTarget.Workspace(owner)
        every { data.secretStorageTarget(SECOND_STORAGE) } returns
          AuthorizationTarget.Workspace(
            WorkspaceAndOrganization(
              UUID(
                0,
                6,
              ),
              if (foreignStore) UUID(0, 99) else ORGANIZATION,
            ),
          )
        if (missing) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
          every { data.organization(ORGANIZATION) } throws AuthorizationResourceNotFoundException()
          every { data.secretStorageTarget(STORAGE) } throws AuthorizationResourceNotFoundException()
        }
        if (lookupError) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("secret scope unavailable")
          every { data.organization(ORGANIZATION) } throws java.io.IOException("secret scope unavailable")
          every { data.secretStorageTarget(STORAGE) } throws java.io.IOException("secret metadata unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(
            grants,
            RoleResolver(
              mockk(),
              mockk(),
              mockk(),
              mockk(),
            ),
            security,
          ),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, "io.airbyte.server.auth.${scenario.rule}")
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected != 200) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200 &&
          scenario.path.endsWith("/get")
        ) {
          assertTrue(
            response.body().contains(if (scenario.storageRead) "credential" else "configuration"),
            response.body(),
          )
        }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val read: Boolean = false,
    val migrate: Boolean = false,
    val storageRead: Boolean = false,
  )

  private fun scenarios() =
    listOf(
      Scenario(
        "/api/v1/secret_storage/create",
        """{"scopeType":"workspace","scopeId":"$WORKSPACE","secretStorageType":"local_testing","descriptor":"fixture","isConfiguredFromEnvironment":true}""",
        "SecretStorageManageRule",
      ),
      Scenario("/api/v1/secret_storage/delete", """{"secretStorageId":"$STORAGE"}""", "SecretStorageDeleteRule"),
      Scenario("/api/v1/secret_storage/list", """{"scopeType":"workspace","scopeId":"$WORKSPACE"}""", "SecretStorageManageRule"),
      Scenario("/api/v1/secret_storage/get", """{"secretStorageId":"$STORAGE"}""", "SecretStorageReadRule", read = true, storageRead = true),
      Scenario(
        "/api/v1/secret_storage/migrate",
        """{"scopeType":"workspace","scopeId":"$WORKSPACE","fromSecretStorageId":"$STORAGE","toSecretStorageId":"$SECOND_STORAGE"}""",
        "SecretStorageMigrationRule",
        migrate = true,
      ),
      Scenario(
        "/api/v1/secrets_persistence_config/create_or_update",
        """{"scope":"organization","scopeId":"$ORGANIZATION","secretPersistenceType":"testing","configuration":{}}""",
        "SecretsPersistenceWriteRule",
      ),
      Scenario(
        "/api/v1/secrets_persistence_config/get",
        """{"scopeType":"organization","scopeId":"$ORGANIZATION"}""",
        "SecretsPersistenceReadRule",
        read = true,
      ),
    )

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val STORAGE: UUID = UUID(0, 3)
    val SECOND_STORAGE: UUID = UUID(0, 4)
    val USER: UUID = UUID(0, 5)
  }
}
