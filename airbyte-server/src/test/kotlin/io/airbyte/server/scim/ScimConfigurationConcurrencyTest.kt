/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.OrganizationDomainVerificationRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.GroupMember
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.OrganizationDomainVerification
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.PermissionServiceDataImpl
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationMethod
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.airbyte.domain.models.scim.ScimConfigurationConflictException
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimIdpProvider
import io.airbyte.domain.models.scim.ScimUserWrite
import io.airbyte.domain.services.scim.ScimAccessGate
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.domain.services.scim.ScimTokenService
import io.airbyte.domain.services.scim.ScimUserLifecycleService
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.transaction.TransactionOperations
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

class ScimConfigurationConcurrencyTest {
  @AfterEach
  fun cleanUp() {
    jooqDslContext.deleteFrom(Tables.GROUP_MEMBER).execute()
    jooqDslContext.deleteFrom(Tables.PERMISSION).execute()
    jooqDslContext.deleteFrom(Tables.SCIM_RESOURCE_MAPPING).execute()
    jooqDslContext.deleteFrom(Tables.GROUP).execute()
    jooqDslContext.deleteFrom(Tables.WORKSPACE).execute()
    jooqDslContext.deleteFrom(Tables.DATAPLANE_GROUP).execute()
    jooqDslContext.deleteFrom(Tables.SCIM_CONFIGURATION).execute()
    jooqDslContext.deleteFrom(Tables.ORGANIZATION).execute()
    jooqDslContext.deleteFrom(Tables.USER).execute()
  }

  @Test
  fun `concurrent initial enables issue exactly one raw token`() {
    val organization =
      organizationRepository.save(
        Organization(name = "concurrent-enable", email = "concurrent-enable@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val userId = UserId(UUID.randomUUID())
    insertUser(userId.value)

    val tokenService = ScimTokenService()
    val service = createService(organizationId, tokenService)
    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val results: List<Future<ScimConfigurationRead>> =
        List(2) {
          executor.submit(
            Callable {
              start.await()
              service.enable(organizationId, ScimIdpProvider.OKTA, userId)
            },
          )
        }
      start.countDown()
      val responses = results.map { it.get(10, TimeUnit.SECONDS) }

      assertThat(responses.mapNotNull { it.token }).hasSize(1)
      val rawToken = responses.single { it.token != null }.token!!
      val stored = scimConfigurationRepository.findByOrganizationId(organizationId.value)
      assertThat(stored).isNotNull
      assertThat(stored!!.tokenHash).isEqualTo(tokenService.hashToken(rawToken))
      assertThat(scimConfigurationRepository.count()).isEqualTo(1)
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent rotations are serialized and leave one returned token active`() {
    val organization =
      organizationRepository.save(
        Organization(name = "concurrent-rotate", email = "concurrent-rotate@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val userId = UserId(UUID.randomUUID())
    insertUser(userId.value)
    val tokenService = ScimTokenService()
    val service = createService(organizationId, tokenService)
    service.enable(organizationId, ScimIdpProvider.OKTA, userId)

    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val results =
        List(2) {
          executor.submit(
            Callable {
              start.await()
              service.rotateToken(organizationId, userId)
            },
          )
        }
      start.countDown()
      val tokens = results.map { it.get(10, TimeUnit.SECONDS).token!! }

      assertThat(tokens).hasSize(2).doesNotHaveDuplicates()
      val stored = scimConfigurationRepository.findByOrganizationId(organizationId.value)
      assertThat(stored).isNotNull
      assertThat(tokens.map(tokenService::hashToken)).contains(stored!!.tokenHash)
      assertThat(scimConfigurationRepository.count()).isEqualTo(1)
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  fun `re-enable reconciles only inactive Users in the configured organization and reapplies guards`() {
    val actorId = UUID.randomUUID()
    insertUser(actorId)
    val organizationA =
      organizationRepository.save(
        Organization(name = "re-enable-a", email = "re-enable-a@example.com"),
      )
    val organizationB =
      organizationRepository.save(
        Organization(name = "re-enable-b", email = "re-enable-b@example.com"),
      )
    val organizationIdA = OrganizationId(organizationA.id!!)
    val organizationIdB = OrganizationId(organizationB.id!!)
    verifyDomain(organizationIdA.value, "example.com")
    verifyDomain(organizationIdB.value, "example.com")
    val tokenService = ScimTokenService()
    val serviceA = createService(organizationIdA, tokenService)
    val serviceB = createService(organizationIdB, tokenService)
    val oldTokenA = serviceA.enable(organizationIdA, ScimIdpProvider.OKTA, UserId(actorId)).token!!
    val tokenB = serviceB.enable(organizationIdB, ScimIdpProvider.OKTA, UserId(actorId)).token!!
    val configurationA = scimConfigurationRepository.findByOrganizationId(organizationIdA.value)!!
    val configurationB = scimConfigurationRepository.findByOrganizationId(organizationIdB.value)!!
    val contextA =
      ScimAuthenticationContext(configurationA.id!!, organizationIdA, tokenService.hashToken(oldTokenA))
    val contextB =
      ScimAuthenticationContext(configurationB.id!!, organizationIdB, tokenService.hashToken(tokenB))
    val inactiveA =
      scimMutationService.execute(contextA) {
        scimUserLifecycleService.create(
          configurationA.id!!,
          organizationIdA.value,
          userInput(active = false, email = "shared-re-enable@example.com"),
        )
      }
    val activeB =
      scimMutationService.execute(contextB) {
        scimUserLifecycleService.create(
          configurationB.id!!,
          organizationIdB.value,
          userInput(active = true, email = "shared-re-enable@example.com"),
        )
      }
    val activeA =
      scimMutationService.execute(contextA) {
        scimUserLifecycleService.create(
          configurationA.id!!,
          organizationIdA.value,
          userInput(active = true, email = "active-re-enable@example.com"),
        )
      }
    assertThat(activeB.userId).isEqualTo(inactiveA.userId)

    val inactiveWorkspaceA = workspace(organizationIdA.value, "inactive-re-enable-a")
    val inactiveWorkspaceB = workspace(organizationIdB.value, "inactive-re-enable-b")
    val activeWorkspaceA = workspace(organizationIdA.value, "active-re-enable-a")
    val inactiveGroupA = group(organizationIdA.value, "Inactive Re-enable A")
    val inactiveGroupB = group(organizationIdB.value, "Inactive Re-enable B")
    val activeGroupA = group(organizationIdA.value, "Active Re-enable A")
    val managedGroupA = group(organizationIdA.value, "Managed Re-enable A")
    val managedGroupMapping =
      scimResourceMappingRepository.save(
        ScimResourceMapping(
          scimConfigurationId = configurationA.id!!,
          organizationId = organizationIdA.value,
          resourceType = ScimResourceType.GROUP,
          groupId = managedGroupA,
          attributes = objectMapper.createObjectNode(),
        ),
      )
    permissionRepository.save(
      Permission(userId = inactiveA.userId, workspaceId = inactiveWorkspaceB, permissionType = PermissionType.workspace_admin),
    )
    groupMemberRepository.save(GroupMember(groupId = inactiveGroupB, userId = inactiveA.userId))
    permissionRepository.save(
      Permission(userId = activeA.userId, workspaceId = activeWorkspaceA, permissionType = PermissionType.workspace_admin),
    )
    groupMemberRepository.save(GroupMember(groupId = activeGroupA, userId = activeA.userId))

    serviceA.disable(organizationIdA, UserId(actorId))
    permissionRepository.save(
      Permission(
        userId = inactiveA.userId,
        organizationId = organizationIdA.value,
        permissionType = PermissionType.organization_admin,
      ),
    )
    permissionRepository.save(
      Permission(userId = inactiveA.userId, workspaceId = inactiveWorkspaceA, permissionType = PermissionType.workspace_admin),
    )
    groupMemberRepository.save(GroupMember(groupId = inactiveGroupA, userId = inactiveA.userId))
    val mappingIdsBefore =
      scimResourceMappingRepository
        .findAllUsers(configurationA.id!!, organizationIdA.value)
        .map { it.id }
        .toSet()

    val reenabled = serviceA.enable(organizationIdA, ScimIdpProvider.OKTA, UserId(actorId))

    assertThat(reenabled.status).isEqualTo(io.airbyte.domain.models.scim.ScimConfigurationStatus.ENABLED)
    assertThat(reenabled.idpProvider).isEqualTo(ScimIdpProvider.OKTA)
    assertThat(reenabled.token).isNotNull().isNotEqualTo(oldTokenA)
    val stored = scimConfigurationRepository.findByOrganizationId(organizationIdA.value)!!
    assertThat(stored.enabled).isTrue()
    assertThat(stored.idpProvider).isEqualTo(ScimIdpProvider.OKTA.storageValue)
    assertThat(stored.tokenHash).isEqualTo(tokenService.hashToken(reenabled.token!!))
    assertThat(scimConfigurationRepository.findEnabledByTokenHash(tokenService.hashToken(oldTokenA))).isNull()
    assertThat(scimConfigurationRepository.findEnabledByTokenHash(tokenService.hashToken(reenabled.token!!))?.id)
      .isEqualTo(configurationA.id)
    assertThat(
      scimResourceMappingRepository
        .findAllUsers(configurationA.id!!, organizationIdA.value)
        .map { it.id }
        .toSet(),
    ).isEqualTo(mappingIdsBefore)
    assertThat(
      scimResourceMappingRepository.findGroup(
        managedGroupMapping.id!!,
        configurationA.id!!,
        organizationIdA.value,
      ),
    ).isNotNull
    assertThat(
      scimResourceMappingRepository.findGroupManagementState(managedGroupA, organizationIdA.value)?.enabled,
    ).isTrue()

    assertThat(directPermissionCount(inactiveA.userId, organizationIdA.value)).isZero()
    assertThat(workspacePermissionCount(inactiveA.userId, inactiveWorkspaceA)).isZero()
    assertThat(groupMembershipCount(inactiveA.userId, inactiveGroupA)).isZero()
    assertThat(directPermissionCount(inactiveA.userId, organizationIdB.value)).isEqualTo(1)
    assertThat(workspacePermissionCount(inactiveA.userId, inactiveWorkspaceB)).isEqualTo(1)
    assertThat(groupMembershipCount(inactiveA.userId, inactiveGroupB)).isEqualTo(1)
    assertThat(directPermissionCount(activeA.userId, organizationIdA.value)).isEqualTo(1)
    assertThat(workspacePermissionCount(activeA.userId, activeWorkspaceA)).isEqualTo(1)
    assertThat(groupMembershipCount(activeA.userId, activeGroupA)).isEqualTo(1)

    val guardedPermissionService =
      PermissionServiceDataImpl(
        mockk<WorkspaceService>(),
        permissionRepository,
        scimConfigurationRepository,
        scimResourceMappingRepository,
      )
    assertThatThrownBy {
      guardedPermissionService.createPermission(
        io.airbyte.config
          .Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(inactiveA.userId)
          .withOrganizationId(organizationIdA.value)
          .withPermissionType(io.airbyte.config.Permission.PermissionType.ORGANIZATION_READER),
      )
    }.isInstanceOf(InactiveUserAccessException::class.java)
  }

  @Test
  fun `re-enable failures roll back cleanup and leave the configuration disabled without a token`() {
    val actorId = UUID.randomUUID()
    insertUser(actorId)
    val organization =
      organizationRepository.save(
        Organization(name = "re-enable-rollback", email = "re-enable-rollback@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    verifyDomain(organizationId.value, "example.com")
    val tokenService = ScimTokenService()
    val service = createService(organizationId, tokenService)
    val oldToken = service.enable(organizationId, ScimIdpProvider.OKTA, UserId(actorId)).token!!
    val configuration = scimConfigurationRepository.findByOrganizationId(organizationId.value)!!
    val context =
      ScimAuthenticationContext(configuration.id!!, organizationId, tokenService.hashToken(oldToken))
    val inactive =
      scimMutationService.execute(context) {
        scimUserLifecycleService.create(
          configuration.id!!,
          organizationId.value,
          userInput(active = false, email = "re-enable-rollback-user@example.com"),
        )
      }
    val workspaceId = workspace(organizationId.value, "re-enable-rollback")
    val groupId = group(organizationId.value, "Re-enable Rollback")
    service.disable(organizationId, UserId(actorId))
    permissionRepository.save(
      Permission(
        userId = inactive.userId,
        organizationId = organizationId.value,
        permissionType = PermissionType.organization_admin,
      ),
    )
    permissionRepository.save(
      Permission(userId = inactive.userId, workspaceId = workspaceId, permissionType = PermissionType.workspace_admin),
    )
    groupMemberRepository.save(GroupMember(groupId = groupId, userId = inactive.userId))

    val cleanupFailure = ExpectedReenableFailure("cleanup failed")
    val failingGroupMemberRepository = spyk(groupMemberRepository)
    every {
      failingGroupMemberRepository.deleteByUserIdAndOrganizationId(inactive.userId, organizationId.value)
    } throws cleanupFailure
    val cleanupTokenService = spyk(ScimTokenService())
    val cleanupFailureService =
      configurationService(
        organizationId,
        ScimUserLifecycleService(
          scimResourceMappingRepository,
          scimAirbyteUserRepository,
          permissionRepository,
          failingGroupMemberRepository,
          domainVerificationRepository,
        ),
        cleanupTokenService,
      )

    assertThatThrownBy {
      cleanupFailureService.enable(organizationId, ScimIdpProvider.OKTA, UserId(actorId))
    }.isSameAs(cleanupFailure)
    verify(exactly = 0) { cleanupTokenService.generateToken() }
    assertReenableRollbackState(configuration.id!!, organizationId.value, inactive.userId, workspaceId, groupId)

    val tokenFailure = ExpectedReenableFailure("token hash failed")
    val failingTokenService = mockk<ScimTokenService>()
    every { failingTokenService.generateToken() } returns "replacement-token"
    every { failingTokenService.hashToken("replacement-token") } throws tokenFailure
    val tokenFailureService = configurationService(organizationId, scimUserLifecycleService, failingTokenService)

    assertThatThrownBy {
      tokenFailureService.enable(organizationId, ScimIdpProvider.OKTA, UserId(actorId))
    }.isSameAs(tokenFailure)
    assertReenableRollbackState(configuration.id!!, organizationId.value, inactive.userId, workspaceId, groupId)

    assertThatThrownBy {
      service.enable(organizationId, ScimIdpProvider.OKTA, UserId(UUID.randomUUID()))
    }.isInstanceOf(DataAccessException::class.java)
    assertReenableRollbackState(configuration.id!!, organizationId.value, inactive.userId, workspaceId, groupId)
  }

  @Test
  fun `simultaneous enable rotation and disable preserve a valid lifecycle state`() {
    val organization =
      organizationRepository.save(
        Organization(name = "mixed-lifecycle", email = "mixed-lifecycle@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val userId = UserId(UUID.randomUUID())
    insertUser(userId.value)
    val tokenService = ScimTokenService()
    val service = createService(organizationId, tokenService)

    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(3)

    try {
      val enable =
        executor.submit(
          Callable {
            start.await()
            service.enable(organizationId, ScimIdpProvider.OKTA, userId)
          },
        )
      val rotation =
        executor.submit(
          Callable {
            start.await()
            runCatching { service.rotateToken(organizationId, userId) }
          },
        )
      val disable =
        executor.submit(
          Callable {
            start.await()
            service.disable(organizationId, userId)
          },
        )

      start.countDown()
      val enableResponse = enable.get(10, TimeUnit.SECONDS)
      val rotationResult = rotation.get(10, TimeUnit.SECONDS)
      disable.get(10, TimeUnit.SECONDS)

      assertThat(enableResponse.token).isNotNull()
      rotationResult.exceptionOrNull()?.let {
        assertThat(it).isInstanceOf(ScimConfigurationConflictException::class.java)
      }

      val stored = scimConfigurationRepository.findByOrganizationId(organizationId.value)
      assertThat(stored).isNotNull
      assertThat(scimConfigurationRepository.count()).isEqualTo(1)
      if (stored!!.enabled) {
        val issuedTokens = listOfNotNull(enableResponse.token, rotationResult.getOrNull()?.token)
        assertThat(issuedTokens.map(tokenService::hashToken)).contains(stored.tokenHash)
      } else {
        assertThat(stored.tokenHash).isNull()
        assertThat(stored.tokenIssuedAt).isNull()
        assertThat(stored.tokenIssuedByUserId).isNull()
      }
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent re-enable rotation and disable serialize to one valid lifecycle state`() {
    val organization =
      organizationRepository.save(
        Organization(name = "mixed-re-enable-lifecycle", email = "mixed-re-enable-lifecycle@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val userId = UserId(UUID.randomUUID())
    insertUser(userId.value)
    val tokenService = ScimTokenService()
    val service = createService(organizationId, tokenService)
    service.enable(organizationId, ScimIdpProvider.OKTA, userId)
    service.disable(organizationId, userId)

    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(3)

    try {
      val reenable =
        executor.submit(
          Callable {
            start.await()
            service.enable(organizationId, ScimIdpProvider.OKTA, userId)
          },
        )
      val rotation =
        executor.submit(
          Callable {
            start.await()
            runCatching { service.rotateToken(organizationId, userId) }
          },
        )
      val disable =
        executor.submit(
          Callable {
            start.await()
            service.disable(organizationId, userId)
          },
        )

      start.countDown()
      val reenableResponse = reenable.get(10, TimeUnit.SECONDS)
      val rotationResult = rotation.get(10, TimeUnit.SECONDS)
      disable.get(10, TimeUnit.SECONDS)

      assertThat(reenableResponse.token).isNotNull()
      rotationResult.exceptionOrNull()?.let {
        assertThat(it).isInstanceOf(ScimConfigurationConflictException::class.java)
      }
      val stored = scimConfigurationRepository.findByOrganizationId(organizationId.value)!!
      assertThat(stored.idpProvider).isEqualTo(ScimIdpProvider.OKTA.storageValue)
      assertThat(scimConfigurationRepository.count()).isEqualTo(1)
      if (stored.enabled) {
        val issuedTokens = listOfNotNull(reenableResponse.token, rotationResult.getOrNull()?.token)
        assertThat(issuedTokens.map(tokenService::hashToken)).contains(stored.tokenHash)
      } else {
        assertThat(stored.tokenHash).isNull()
        assertThat(stored.tokenIssuedAt).isNull()
        assertThat(stored.tokenIssuedByUserId).isNull()
      }
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  fun `failed initial enable propagates and rolls back configuration creation`() {
    val organization =
      organizationRepository.save(
        Organization(name = "failed-enable", email = "failed-enable@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val missingUserId = UserId(UUID.randomUUID())
    val service = createService(organizationId, ScimTokenService())

    assertThatThrownBy {
      service.enable(organizationId, ScimIdpProvider.OKTA, missingUserId)
    }.isInstanceOf(DataAccessException::class.java)

    assertThat(scimConfigurationRepository.findByOrganizationId(organizationId.value)).isNull()
  }

  @Test
  fun `failed rotation propagates and preserves the active token`() {
    val organization =
      organizationRepository.save(
        Organization(name = "failed-rotation", email = "failed-rotation@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val userId = UserId(UUID.randomUUID())
    insertUser(userId.value)
    val service = createService(organizationId, ScimTokenService())
    service.enable(organizationId, ScimIdpProvider.OKTA, userId)
    val before = scimConfigurationRepository.findByOrganizationId(organizationId.value)!!

    assertThatThrownBy {
      service.rotateToken(organizationId, UserId(UUID.randomUUID()))
    }.isInstanceOf(DataAccessException::class.java)

    val after = scimConfigurationRepository.findByOrganizationId(organizationId.value)!!
    assertThat(after.enabled).isTrue()
    assertThat(after.tokenHash).isEqualTo(before.tokenHash)
    assertThat(after.tokenIssuedAt).isEqualTo(before.tokenIssuedAt)
    assertThat(after.tokenIssuedByUserId).isEqualTo(before.tokenIssuedByUserId)
  }

  @Test
  fun `failed disable propagates and preserves the enabled configuration`() {
    val organization =
      organizationRepository.save(
        Organization(name = "failed-disable", email = "failed-disable@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val userId = UserId(UUID.randomUUID())
    insertUser(userId.value)
    val service = createService(organizationId, ScimTokenService())
    service.enable(organizationId, ScimIdpProvider.OKTA, userId)
    val before = scimConfigurationRepository.findByOrganizationId(organizationId.value)!!

    assertThatThrownBy {
      service.disable(organizationId, UserId(UUID.randomUUID()))
    }.isInstanceOf(DataAccessException::class.java)

    val after = scimConfigurationRepository.findByOrganizationId(organizationId.value)!!
    assertThat(after.enabled).isTrue()
    assertThat(after.tokenHash).isEqualTo(before.tokenHash)
    assertThat(after.disabledAt).isNull()
    assertThat(after.disabledByUserId).isNull()
  }

  @Test
  fun `rotation that acquires locks first invalidates a waiting mutation`() {
    val organization =
      organizationRepository.save(
        Organization(name = "rotation-vs-mutation", email = "rotation-vs-mutation@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val userId = UserId(UUID.randomUUID())
    insertUser(userId.value)
    val tokenService = ScimTokenService()
    val lifecycleService = createService(organizationId, tokenService)
    val rawToken = lifecycleService.enable(organizationId, ScimIdpProvider.OKTA, userId).token!!
    val configuration = scimConfigurationRepository.findByOrganizationId(organizationId.value)!!
    val oldContext =
      ScimAuthenticationContext(
        configurationId = configuration.id!!,
        organizationId = organizationId,
        tokenHash = tokenService.hashToken(rawToken),
      )
    val mutationService =
      ScimMutationService(
        organizationRepository,
        scimConfigurationRepository,
        configTransactionOperations,
      )
    val lifecycleUpdated = CountDownLatch(1)
    val allowLifecycleCommit = CountDownLatch(1)
    val mutationStarted = CountDownLatch(1)
    val mutationRan = AtomicBoolean(false)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val lifecycle =
        executor.submit(
          Callable {
            configTransactionOperations.executeWrite { _ ->
              assertThat(organizationRepository.findByIdForUpdate(organizationId.value)).isPresent
              val locked = scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value)!!
              val now = OffsetDateTime.now(ZoneOffset.UTC)
              assertThat(
                scimConfigurationRepository.rotateTokenByIdAndOrganizationId(
                  id = locked.id!!,
                  organizationId = organizationId.value,
                  tokenHash = tokenService.hashToken(tokenService.generateToken()),
                  tokenIssuedAt = now,
                  tokenIssuedByUserId = userId.value,
                  updatedAt = now,
                ),
              ).isEqualTo(1)
              lifecycleUpdated.countDown()
              assertThat(allowLifecycleCommit.await(10, TimeUnit.SECONDS)).isTrue()
            }
          },
        )
      assertThat(lifecycleUpdated.await(10, TimeUnit.SECONDS)).isTrue()
      val mutation =
        executor.submit(
          Callable {
            mutationStarted.countDown()
            assertThatThrownBy {
              mutationService.execute(oldContext) { mutationRan.set(true) }
            }.isInstanceOf(ScimAuthenticationException::class.java)
          },
        )
      assertThat(mutationStarted.await(10, TimeUnit.SECONDS)).isTrue()

      allowLifecycleCommit.countDown()
      lifecycle.get(10, TimeUnit.SECONDS)
      mutation.get(10, TimeUnit.SECONDS)

      assertThat(mutationRan).isFalse()
    } finally {
      allowLifecycleCommit.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `disable that acquires locks first invalidates a waiting mutation`() {
    val organization =
      organizationRepository.save(
        Organization(name = "disable-vs-mutation", email = "disable-vs-mutation@example.com"),
      )
    val organizationId = OrganizationId(organization.id!!)
    val userId = UserId(UUID.randomUUID())
    insertUser(userId.value)
    val tokenService = ScimTokenService()
    val lifecycleService = createService(organizationId, tokenService)
    val rawToken = lifecycleService.enable(organizationId, ScimIdpProvider.OKTA, userId).token!!
    val configuration = scimConfigurationRepository.findByOrganizationId(organizationId.value)!!
    val oldContext =
      ScimAuthenticationContext(
        configurationId = configuration.id!!,
        organizationId = organizationId,
        tokenHash = tokenService.hashToken(rawToken),
      )
    val mutationService =
      ScimMutationService(
        organizationRepository,
        scimConfigurationRepository,
        configTransactionOperations,
      )
    val lifecycleUpdated = CountDownLatch(1)
    val allowLifecycleCommit = CountDownLatch(1)
    val mutationStarted = CountDownLatch(1)
    val mutationRan = AtomicBoolean(false)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val lifecycle =
        executor.submit(
          Callable {
            configTransactionOperations.executeWrite { _ ->
              assertThat(organizationRepository.findByIdForUpdate(organizationId.value)).isPresent
              val locked = scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value)!!
              val now = OffsetDateTime.now(ZoneOffset.UTC)
              assertThat(
                scimConfigurationRepository.disableByIdAndOrganizationId(
                  id = locked.id!!,
                  organizationId = organizationId.value,
                  disabledAt = now,
                  disabledByUserId = userId.value,
                  updatedAt = now,
                ),
              ).isEqualTo(1)
              lifecycleUpdated.countDown()
              assertThat(allowLifecycleCommit.await(10, TimeUnit.SECONDS)).isTrue()
            }
          },
        )
      assertThat(lifecycleUpdated.await(10, TimeUnit.SECONDS)).isTrue()
      val mutation =
        executor.submit(
          Callable {
            mutationStarted.countDown()
            assertThatThrownBy {
              mutationService.execute(oldContext) { mutationRan.set(true) }
            }.isInstanceOf(ScimAuthenticationException::class.java)
          },
        )
      assertThat(mutationStarted.await(10, TimeUnit.SECONDS)).isTrue()

      allowLifecycleCommit.countDown()
      lifecycle.get(10, TimeUnit.SECONDS)
      mutation.get(10, TimeUnit.SECONDS)

      assertThat(mutationRan).isFalse()
    } finally {
      allowLifecycleCommit.countDown()
      executor.shutdownNow()
    }
  }

  private fun createService(
    organizationId: OrganizationId,
    tokenService: ScimTokenService,
  ): ScimConfigurationService = configurationService(organizationId, scimUserLifecycleService, tokenService)

  private fun configurationService(
    organizationId: OrganizationId,
    userLifecycleService: ScimUserLifecycleService,
    tokenService: ScimTokenService,
  ): ScimConfigurationService {
    val gate = mockk<ScimAccessGate>()
    every { gate.isAllowed(organizationId) } returns true
    return ScimConfigurationService(
      gate,
      organizationRepository,
      scimConfigurationRepository,
      userLifecycleService,
      tokenService,
      configTransactionOperations,
    )
  }

  private fun userInput(
    active: Boolean,
    email: String,
  ): ScimUserWrite =
    ScimUserWrite(
      userName = email,
      externalId = "external-$email",
      primaryEmail = email,
      active = active,
      attributes =
        objectMapper.createObjectNode().also {
          it.put("displayName", email)
          it
            .putArray("emails")
            .addObject()
            .put("value", email)
            .put("type", "work")
            .put("primary", true)
        },
    )

  private fun workspace(
    organizationId: UUID,
    name: String,
  ): UUID {
    val dataplaneGroupId = UUID.randomUUID()
    jooqDslContext
      .insertInto(Tables.DATAPLANE_GROUP)
      .set(Tables.DATAPLANE_GROUP.ID, dataplaneGroupId)
      .set(Tables.DATAPLANE_GROUP.ORGANIZATION_ID, organizationId)
      .set(Tables.DATAPLANE_GROUP.NAME, "$name dataplane")
      .execute()
    return UUID.randomUUID().also { workspaceId ->
      jooqDslContext
        .insertInto(Tables.WORKSPACE)
        .set(Tables.WORKSPACE.ID, workspaceId)
        .set(Tables.WORKSPACE.NAME, name)
        .set(Tables.WORKSPACE.SLUG, name)
        .set(Tables.WORKSPACE.INITIAL_SETUP_COMPLETE, true)
        .set(Tables.WORKSPACE.TOMBSTONE, false)
        .set(Tables.WORKSPACE.ORGANIZATION_ID, organizationId)
        .set(Tables.WORKSPACE.DATAPLANE_GROUP_ID, dataplaneGroupId)
        .execute()
    }
  }

  private fun group(
    organizationId: UUID,
    name: String,
  ): UUID =
    UUID.randomUUID().also { groupId ->
      jooqDslContext
        .insertInto(Tables.GROUP)
        .set(Tables.GROUP.ID, groupId)
        .set(Tables.GROUP.NAME, name)
        .set(Tables.GROUP.ORGANIZATION_ID, organizationId)
        .execute()
    }

  private fun directPermissionCount(
    userId: UUID,
    organizationId: UUID,
  ): Int =
    jooqDslContext.fetchCount(
      Tables.PERMISSION,
      Tables.PERMISSION.USER_ID
        .eq(userId)
        .and(Tables.PERMISSION.ORGANIZATION_ID.eq(organizationId)),
    )

  private fun workspacePermissionCount(
    userId: UUID,
    workspaceId: UUID,
  ): Int =
    jooqDslContext.fetchCount(
      Tables.PERMISSION,
      Tables.PERMISSION.USER_ID
        .eq(userId)
        .and(Tables.PERMISSION.WORKSPACE_ID.eq(workspaceId)),
    )

  private fun groupMembershipCount(
    userId: UUID,
    groupId: UUID,
  ): Int =
    jooqDslContext.fetchCount(
      Tables.GROUP_MEMBER,
      Tables.GROUP_MEMBER.USER_ID
        .eq(userId)
        .and(Tables.GROUP_MEMBER.GROUP_ID.eq(groupId)),
    )

  private fun assertReenableRollbackState(
    configurationId: UUID,
    organizationId: UUID,
    userId: UUID,
    workspaceId: UUID,
    groupId: UUID,
  ) {
    val configuration = scimConfigurationRepository.findByOrganizationId(organizationId)!!
    assertThat(configuration.id).isEqualTo(configurationId)
    assertThat(configuration.enabled).isFalse()
    assertThat(configuration.tokenHash).isNull()
    assertThat(configuration.tokenIssuedAt).isNull()
    assertThat(configuration.tokenIssuedByUserId).isNull()
    assertThat(configuration.disabledAt).isNotNull()
    assertThat(scimResourceMappingRepository.findAllUsers(configurationId, organizationId)).hasSize(1)
    assertThat(directPermissionCount(userId, organizationId)).isEqualTo(1)
    assertThat(workspacePermissionCount(userId, workspaceId)).isEqualTo(1)
    assertThat(groupMembershipCount(userId, groupId)).isEqualTo(1)
  }

  /** SCIM provisioning is gated on verified domain ownership, so provisioning fixtures need one. */
  private fun verifyDomain(
    organizationId: UUID,
    domain: String,
  ) {
    domainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = organizationId,
        domain = domain,
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.verified,
        verificationToken = UUID.randomUUID().toString(),
        dnsRecordName = "_airbyte-verification.$domain",
        dnsRecordPrefix = "_airbyte-verification",
        verifiedAt = OffsetDateTime.now(),
      ),
    )
  }

  private fun insertUser(userId: UUID) {
    jooqDslContext
      .insertInto(Tables.USER)
      .set(Tables.USER.ID, userId)
      .set(Tables.USER.NAME, "SCIM test user")
      .set(Tables.USER.EMAIL, "$userId@example.com")
      .execute()
  }

  private class ExpectedReenableFailure(
    message: String,
  ) : RuntimeException(message)

  companion object {
    private lateinit var context: ApplicationContext
    private lateinit var jooqDslContext: DSLContext
    private lateinit var objectMapper: ObjectMapper
    private lateinit var organizationRepository: OrganizationRepository
    private lateinit var scimConfigurationRepository: ScimConfigurationRepository
    private lateinit var scimResourceMappingRepository: ScimResourceMappingRepository
    private lateinit var scimAirbyteUserRepository: ScimAirbyteUserRepository
    private lateinit var domainVerificationRepository: OrganizationDomainVerificationRepository
    private lateinit var permissionRepository: PermissionRepository
    private lateinit var groupMemberRepository: GroupMemberRepository
    private lateinit var scimUserLifecycleService: ScimUserLifecycleService
    private lateinit var scimMutationService: ScimMutationService
    private lateinit var configTransactionOperations: TransactionOperations<Connection>

    private val container: PostgreSQLContainer<*> =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker")

    @BeforeAll
    @JvmStatic
    fun setUpDatabase() {
      container.start()
      container.createConnection("").use { }
      context =
        ApplicationContext.run(
          PropertySource.of(
            "scim-concurrency-test",
            mapOf(
              "datasources.config.driverClassName" to "org.postgresql.Driver",
              "datasources.config.db-type" to "postgres",
              "datasources.config.dialect" to "POSTGRES",
              "datasources.config.url" to container.jdbcUrl,
              "datasources.config.username" to container.username,
              "datasources.config.password" to container.password,
            ),
          ),
        )

      val dataSource =
        (context.getBean(DataSource::class.java, Qualifiers.byName("config")) as DelegatingDataSource)
          .targetDataSource
      jooqDslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
      TestDatabaseProviders(dataSource, jooqDslContext).createNewConfigsDatabase()

      organizationRepository = context.getBean(OrganizationRepository::class.java)
      scimConfigurationRepository = context.getBean(ScimConfigurationRepository::class.java)
      scimResourceMappingRepository = context.getBean(ScimResourceMappingRepository::class.java)
      scimAirbyteUserRepository = context.getBean(ScimAirbyteUserRepository::class.java)
      domainVerificationRepository = context.getBean(OrganizationDomainVerificationRepository::class.java)
      permissionRepository = context.getBean(PermissionRepository::class.java)
      groupMemberRepository = context.getBean(GroupMemberRepository::class.java)
      scimUserLifecycleService = context.getBean(ScimUserLifecycleService::class.java)
      scimMutationService = context.getBean(ScimMutationService::class.java)
      objectMapper = context.getBean(ObjectMapper::class.java)
      @Suppress("UNCHECKED_CAST")
      configTransactionOperations =
        context.getBean(TransactionOperations::class.java, Qualifiers.byName("config")) as TransactionOperations<Connection>
    }

    @AfterAll
    @JvmStatic
    fun tearDownDatabase() {
      context.close()
      container.close()
    }
  }
}
