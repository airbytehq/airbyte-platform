/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.User
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.repositories.ApplicationRepository
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.GroupMemberWithUserInfoRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.repositories.GroupWithMemberCountRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.UserInvitationRepository
import io.airbyte.data.repositories.entities.GroupMember
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.ScimAirbyteUser
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.ApplicationServiceDataImpl
import io.airbyte.data.services.impls.data.GroupServiceDataImpl
import io.airbyte.data.services.impls.data.PermissionServiceDataImpl
import io.airbyte.data.services.impls.data.UserInvitationServiceDataImpl
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.InvitationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimUserConflictException
import io.airbyte.domain.models.scim.ScimUserFilterAttribute
import io.airbyte.domain.models.scim.ScimUserFilterClause
import io.airbyte.domain.models.scim.ScimUserNotFoundException
import io.airbyte.domain.models.scim.ScimUserWrite
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.domain.services.scim.ScimUserLifecycleService
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.http.HttpRequest
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.micronaut.security.token.jwt.validator.ReactiveJsonWebTokenValidator
import io.micronaut.transaction.TransactionOperations
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import reactor.core.publisher.Mono
import java.net.URI
import java.sql.Connection
import java.time.Instant
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

class ScimUserLifecycleIntegrationTest {
  @AfterEach
  fun cleanUp() {
    jooq.deleteFrom(Tables.APPLICATION).execute()
    jooq.deleteFrom(Tables.USER_INVITATION).execute()
    jooq.deleteFrom(Tables.GROUP_MEMBER).execute()
    jooq.deleteFrom(Tables.PERMISSION).execute()
    jooq.deleteFrom(Tables.SCIM_RESOURCE_MAPPING).execute()
    jooq.deleteFrom(Tables.GROUP).execute()
    jooq.deleteFrom(Tables.WORKSPACE).execute()
    jooq.deleteFrom(Tables.DATAPLANE_GROUP).execute()
    jooq.deleteFrom(Tables.SCIM_CONFIGURATION).execute()
    jooq.deleteFrom(Tables.ORGANIZATION).execute()
    jooq.deleteFrom(Tables.AUTH_USER).execute()
    jooq.deleteFrom(Tables.USER).execute()
  }

  @Test
  fun `deactivate delete and reactivate affect only the target organization`() {
    val tenantA = tenant("tenant-a")
    val tenantB = tenant("tenant-b")
    val createdA = mutationService.execute(tenantA.context) { lifecycleService.create(tenantA.configurationId, tenantA.organizationId, input(true)) }
    val createdB = mutationService.execute(tenantB.context) { lifecycleService.create(tenantB.configurationId, tenantB.organizationId, input(true)) }
    assertThat(createdA.createdAt).isEqualTo(createdA.updatedAt)
    assertThat(createdB.userId).isEqualTo(createdA.userId)
    assertThat(
      jooq
        .select(Tables.USER.STATUS)
        .from(Tables.USER)
        .where(Tables.USER.ID.eq(createdA.userId))
        .fetchOne(Tables.USER.STATUS),
    ).isNull()
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.USER_ID.eq(createdA.userId))).isZero()
    assertThatThrownBy { lifecycleService.get(tenantA.configurationId, tenantA.organizationId, createdA.userId) }
      .isInstanceOf(ScimUserNotFoundException::class.java)

    val workspaceA = workspace(tenantA.organizationId, "workspace-a")
    val workspaceB = workspace(tenantB.organizationId, "workspace-b")
    val groupA = group(tenantA.organizationId, "Engineering A")
    val groupB = group(tenantB.organizationId, "Engineering B")
    permissionRepository.save(Permission(userId = createdA.userId, workspaceId = workspaceA, permissionType = PermissionType.workspace_admin))
    permissionRepository.save(Permission(userId = createdA.userId, workspaceId = workspaceB, permissionType = PermissionType.workspace_admin))
    groupMemberRepository.save(GroupMember(groupId = groupA, userId = createdA.userId))
    groupMemberRepository.save(GroupMember(groupId = groupB, userId = createdA.userId))
    mappingRepository.save(groupMapping(tenantA, groupA, "Engineering A"))

    val withGroups = lifecycleService.get(tenantA.configurationId, tenantA.organizationId, createdA.id)
    assertThat(withGroups.groups.map { it.displayName }).containsExactly("Engineering A")

    val deactivated =
      mutationService.execute(tenantA.context) {
        lifecycleService.replace(tenantA.configurationId, tenantA.organizationId, createdA.id, input(false))
      }
    assertThat(deactivated.active).isFalse()
    assertThat(directPermissionCount(createdA.userId, tenantA.organizationId)).isZero()
    assertThat(workspacePermissionCount(createdA.userId, workspaceA)).isZero()
    assertThat(groupMembershipCount(createdA.userId, groupA)).isZero()
    assertThat(directPermissionCount(createdA.userId, tenantB.organizationId)).isEqualTo(1)
    assertThat(workspacePermissionCount(createdA.userId, workspaceB)).isEqualTo(1)
    assertThat(groupMembershipCount(createdA.userId, groupB)).isEqualTo(1)

    val reactivated =
      mutationService.execute(tenantA.context) {
        lifecycleService.replace(tenantA.configurationId, tenantA.organizationId, createdA.id, input(true))
      }
    assertThat(reactivated.active).isTrue()
    assertThat(directPermissionCount(createdA.userId, tenantA.organizationId)).isEqualTo(1)
    assertThat(workspacePermissionCount(createdA.userId, workspaceA)).isZero()
    assertThat(groupMembershipCount(createdA.userId, groupA)).isZero()

    mutationService.execute(tenantA.context) {
      lifecycleService.delete(tenantA.configurationId, tenantA.organizationId, createdA.id)
    }
    assertThat(mappingRepository.findUser(createdA.id, tenantA.configurationId, tenantA.organizationId)).isNull()
    assertThat(userRepository.findById(createdA.userId)).isPresent
    assertThat(mappingRepository.findUser(createdB.id, tenantB.configurationId, tenantB.organizationId)).isNotNull
    assertThat(directPermissionCount(createdA.userId, tenantB.organizationId)).isEqualTo(1)
  }

  @Test
  fun `inactive guards are organization scoped and explicit SCIM disable releases them`() {
    val tenantA = tenant("inactive-guard-a")
    val tenantB = tenant("inactive-guard-b")
    val createdA = mutationService.execute(tenantA.context) { lifecycleService.create(tenantA.configurationId, tenantA.organizationId, input(true)) }
    val createdB = mutationService.execute(tenantB.context) { lifecycleService.create(tenantB.configurationId, tenantB.organizationId, input(true)) }
    val workspaceA = workspace(tenantA.organizationId, "inactive-guard-workspace-a")
    val groupA = group(tenantA.organizationId, "Inactive Guard Group A")
    val groupB = group(tenantB.organizationId, "Inactive Guard Group B")
    val workspaceService = mockk<WorkspaceService>()
    every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceA) } returns Optional.of(tenantA.organizationId)
    val guardedPermissionService =
      PermissionServiceDataImpl(
        workspaceService,
        permissionRepository,
        configurationRepository,
        mappingRepository,
      )

    mutationService.execute(tenantA.context) {
      lifecycleService.replace(tenantA.configurationId, tenantA.organizationId, createdA.id, input(false))
    }

    assertThatThrownBy {
      guardedPermissionService.createPermission(
        io.airbyte.config
          .Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(createdA.userId)
          .withOrganizationId(tenantA.organizationId)
          .withPermissionType(io.airbyte.config.Permission.PermissionType.ORGANIZATION_READER),
      )
    }.isInstanceOf(InactiveUserAccessException::class.java)
    assertThatThrownBy {
      guardedPermissionService.createPermission(
        io.airbyte.config
          .Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(createdA.userId)
          .withWorkspaceId(workspaceA)
          .withPermissionType(io.airbyte.config.Permission.PermissionType.WORKSPACE_READER),
      )
    }.isInstanceOf(InactiveUserAccessException::class.java)
    assertThatThrownBy {
      groupService.addGroupMember(GroupId(groupA), UserId(createdA.userId), OrganizationId(tenantA.organizationId))
    }.isInstanceOf(InactiveUserAccessException::class.java)

    groupService.addGroupMember(GroupId(groupB), UserId(createdB.userId), OrganizationId(tenantB.organizationId))
    assertThat(groupMembershipCount(createdA.userId, groupB)).isEqualTo(1)

    val now = OffsetDateTime.now()
    assertThat(
      configurationRepository.disableByIdAndOrganizationId(
        tenantA.configurationId,
        tenantA.organizationId,
        now,
        createdA.userId,
        now,
      ),
    ).isEqualTo(1)
    guardedPermissionService.createPermission(
      io.airbyte.config
        .Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(createdA.userId)
        .withOrganizationId(tenantA.organizationId)
        .withPermissionType(io.airbyte.config.Permission.PermissionType.ORGANIZATION_MEMBER),
    )
    groupService.addGroupMember(GroupId(groupA), UserId(createdA.userId), OrganizationId(tenantA.organizationId))

    assertThat(directPermissionCount(createdA.userId, tenantA.organizationId)).isEqualTo(1)
    assertThat(groupMembershipCount(createdA.userId, groupA)).isEqualTo(1)
  }

  @Test
  fun `organization and workspace invitation acceptance use inactive guards until explicit disable`() {
    val tenant = tenant("invitation-guard")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }
    val workspaceId = workspace(tenant.organizationId, "invitation-guard-workspace")
    val organizationInvitation = invitation(created.userId, tenant.organizationId, ScopeType.organization, PermissionType.organization_reader)
    val workspaceInvitation = invitation(created.userId, workspaceId, ScopeType.workspace, PermissionType.workspace_admin)
    val workspaceService = mockk<WorkspaceService>()
    every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.of(tenant.organizationId)
    val guardedPermissionService =
      PermissionServiceDataImpl(
        workspaceService,
        permissionRepository,
        configurationRepository,
        mappingRepository,
      )
    val invitationService = UserInvitationServiceDataImpl(userInvitationRepository, permissionRepository, guardedPermissionService)

    mutationService.execute(tenant.context) {
      lifecycleService.replace(tenant.configurationId, tenant.organizationId, created.id, input(false))
    }

    assertThatThrownBy { invitationService.acceptUserInvitation(organizationInvitation.inviteCode, created.userId) }
      .isInstanceOf(InactiveUserAccessException::class.java)
    assertThatThrownBy { invitationService.acceptUserInvitation(workspaceInvitation.inviteCode, created.userId) }
      .isInstanceOf(InactiveUserAccessException::class.java)
    assertThat(userInvitationRepository.findByInviteCode(organizationInvitation.inviteCode).orElseThrow().status)
      .isEqualTo(InvitationStatus.pending)
    assertThat(userInvitationRepository.findByInviteCode(workspaceInvitation.inviteCode).orElseThrow().status)
      .isEqualTo(InvitationStatus.pending)

    val now = OffsetDateTime.now()
    configurationRepository.disableByIdAndOrganizationId(
      tenant.configurationId,
      tenant.organizationId,
      now,
      created.userId,
      now,
    )
    invitationService.acceptUserInvitation(organizationInvitation.inviteCode, created.userId)
    invitationService.acceptUserInvitation(workspaceInvitation.inviteCode, created.userId)

    assertThat(directPermissionCount(created.userId, tenant.organizationId)).isEqualTo(1)
    assertThat(workspacePermissionCount(created.userId, workspaceId)).isEqualTo(1)
  }

  @Test
  fun `existing browser SSO personal and developer credentials lose only target organization roles on the next request`() {
    val tenantA = tenant("credential-a")
    val tenantB = tenant("credential-b")
    val createdA = mutationService.execute(tenantA.context) { lifecycleService.create(tenantA.configurationId, tenantA.organizationId, input(true)) }
    val createdB = mutationService.execute(tenantB.context) { lifecycleService.create(tenantB.configurationId, tenantB.organizationId, input(true)) }
    val authUserId = "existing-user-credential"
    userPersistence.writeAuthUser(createdA.userId, authUserId, AuthProvider.KEYCLOAK)
    val groupA = group(tenantA.organizationId, "Credential Group A")
    groupService.addGroupMember(GroupId(groupA), UserId(createdA.userId), OrganizationId(tenantA.organizationId))
    permissionRepository.save(
      Permission(
        groupId = groupA,
        organizationId = tenantA.organizationId,
        permissionType = PermissionType.organization_admin,
      ),
    )
    val authenticatedUser =
      AuthenticatedUser()
        .withUserId(createdA.userId)
        .withAuthUserId(authUserId)
        .withEmail(createdA.primaryEmail)
    val personalApplication = applicationService.createApplication(authenticatedUser, "Personal token")
    val developerApplication = applicationService.createApplication(authenticatedUser, "Developer application")
    val personalToken = applicationService.getToken(personalApplication.clientId, personalApplication.clientSecret)
    val developerToken = applicationService.getToken(developerApplication.clientId, developerApplication.clientSecret)
    val browserToken =
      jwtTokenGenerator
        .generateToken(
          mapOf(
            "iss" to "http://test-url.com",
            "aud" to "airbyte-server",
            "sub" to authUserId,
            "exp" to Instant.now().plus(10, ChronoUnit.MINUTES).epochSecond,
          ),
        ).orElseThrow()
    val credentials = listOf(browserToken, personalToken, developerToken)
    val permissionHandler = PermissionHandler(null, mockk<WorkspaceService>(), null, permissionService)
    val authenticationHeaderResolver = context.getBean(AuthenticationHeaderResolver::class.java)
    val roleResolver = RoleResolver(authenticationHeaderResolver, mockk<CurrentUserService>(), null, permissionHandler)

    val targetRolesBefore = credentials.map { rolesForCredential(roleResolver, it, tenantA.organizationId) }
    val otherRolesBefore = credentials.map { rolesForCredential(roleResolver, it, tenantB.organizationId) }

    mutationService.execute(tenantA.context) {
      lifecycleService.replace(tenantA.configurationId, tenantA.organizationId, createdA.id, input(false))
    }

    val targetRolesAfter = credentials.map { rolesForCredential(roleResolver, it, tenantA.organizationId) }
    val otherRolesAfter = credentials.map { rolesForCredential(roleResolver, it, tenantB.organizationId) }
    assertThat(targetRolesBefore).allSatisfy { assertThat(it).contains(AuthRoleConstants.ORGANIZATION_MEMBER) }
    assertThat(otherRolesBefore).allSatisfy { assertThat(it).contains(AuthRoleConstants.ORGANIZATION_MEMBER) }
    assertThat(targetRolesAfter).allSatisfy { assertThat(it).containsExactly(AuthRoleConstants.AUTHENTICATED_USER) }
    assertThat(otherRolesAfter).isEqualTo(otherRolesBefore)
    assertThat(groupMembershipCount(createdA.userId, groupA)).isZero()
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isEqualTo(1)
    assertThat(createdB.userId).isEqualTo(createdA.userId)
  }

  private fun rolesForCredential(
    roleResolver: RoleResolver,
    token: String,
    organizationId: UUID,
  ): Set<String> {
    val authentication = Mono.from(jwtTokenValidator.validateToken(token, null)).block() ?: error("credential did not authenticate")
    return roleResolver
      .newRequest()
      .withClaims(authentication.name, authentication.attributes)
      .withOrg(organizationId)
      .roles()
  }

  @Test
  fun `concurrent permission grant is serialized with inactive POST for a reused User`() {
    val tenant = tenant("permission-first-create-race")
    val existingUser = userRepository.save(ScimAirbyteUser(name = "Existing User", email = "permission-first-create@example.com"))
    permissionRepository.save(
      Permission(
        userId = existingUser.id,
        organizationId = tenant.organizationId,
        permissionType = PermissionType.organization_member,
      ),
    )
    val workspaceId = workspace(tenant.organizationId, "permission-first-create-race-workspace")
    val mappingCheckedMissing = CountDownLatch(1)
    val releaseGrant = CountDownLatch(1)
    val scimConfigurationLockAttempted = CountDownLatch(1)
    val scimConfigurationLocked = CountDownLatch(1)
    val cleanupReached = CountDownLatch(1)
    val releaseCleanup = CountDownLatch(1)
    val concurrentMappingRepository = spyk(mappingRepository)
    every {
      concurrentMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(existingUser.id, tenant.organizationId)
    } answers {
      val mapping = callOriginal()
      check(mapping == null)
      mappingCheckedMissing.countDown()
      check(releaseGrant.await(10, TimeUnit.SECONDS))
      mapping
    }
    val workspaceService = mockk<WorkspaceService>()
    every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.of(tenant.organizationId)
    val guardedPermissionService =
      PermissionServiceDataImpl(
        workspaceService,
        permissionRepository,
        configurationRepository,
        concurrentMappingRepository,
      )
    val concurrentConfigurationRepository = spyk(configurationRepository)
    every {
      concurrentConfigurationRepository.findByIdAndOrganizationIdForUpdate(tenant.configurationId, tenant.organizationId)
    } answers {
      scimConfigurationLockAttempted.countDown()
      val configuration = callOriginal()
      scimConfigurationLocked.countDown()
      configuration
    }
    val cleanupPermissionRepository = spyk(permissionRepository)
    every {
      cleanupPermissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(existingUser.id, tenant.organizationId)
    } answers {
      val deleted = callOriginal()
      cleanupReached.countDown()
      check(releaseCleanup.await(10, TimeUnit.SECONDS))
      deleted
    }
    val concurrentLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        userRepository,
        cleanupPermissionRepository,
        groupMemberRepository,
      )
    val concurrentMutationService = ScimMutationService(organizationRepository, concurrentConfigurationRepository, transactions)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val grantFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            transactions.executeWrite {
              guardedPermissionService.createPermission(
                io.airbyte.config
                  .Permission()
                  .withPermissionId(UUID.randomUUID())
                  .withUserId(existingUser.id)
                  .withWorkspaceId(workspaceId)
                  .withPermissionType(io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN),
              )
              Unit
            }
          }
        }
      check(mappingCheckedMissing.await(10, TimeUnit.SECONDS))
      val inactivePostFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            concurrentMutationService.execute(tenant.context) {
              concurrentLifecycle.create(
                tenant.configurationId,
                tenant.organizationId,
                input(false, existingUser.email),
              )
            }
            Unit
          }
        }
      check(scimConfigurationLockAttempted.await(10, TimeUnit.SECONDS))

      val scimLockedBeforeGrantCompleted = scimConfigurationLocked.await(2, TimeUnit.SECONDS)
      if (scimLockedBeforeGrantCompleted) {
        check(cleanupReached.await(10, TimeUnit.SECONDS))
      }
      releaseGrant.countDown()
      val grantResult = grantFuture.get(30, TimeUnit.SECONDS)
      check(cleanupReached.await(10, TimeUnit.SECONDS))
      releaseCleanup.countDown()
      val inactivePostResult = inactivePostFuture.get(30, TimeUnit.SECONDS)

      assertThat(scimLockedBeforeGrantCompleted).isFalse()
      assertThat(grantResult.isSuccess).isTrue()
      assertThat(inactivePostResult.isSuccess).isTrue()
      assertThat(directPermissionCount(existingUser.id, tenant.organizationId)).isZero()
      assertThat(workspacePermissionCount(existingUser.id, workspaceId)).isZero()
      assertThat(mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId).single().userActive).isFalse()
    } finally {
      releaseGrant.countDown()
      releaseCleanup.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent Group addition is serialized with inactive POST for a reused User`() {
    val tenant = tenant("group-first-create-race")
    val existingUser = userRepository.save(ScimAirbyteUser(name = "Existing User", email = "group-first-create@example.com"))
    permissionRepository.save(
      Permission(
        userId = existingUser.id,
        organizationId = tenant.organizationId,
        permissionType = PermissionType.organization_member,
      ),
    )
    val groupId = group(tenant.organizationId, "First Create Race Group")
    val membershipChecked = CountDownLatch(1)
    val releaseAddition = CountDownLatch(1)
    val scimOrganizationLockAttempted = CountDownLatch(1)
    val cleanupReached = CountDownLatch(1)
    val releaseCleanup = CountDownLatch(1)
    val concurrentPermissionRepository = spyk(permissionRepository)
    every {
      concurrentPermissionRepository.existsByUserIdAndOrganizationId(existingUser.id, tenant.organizationId)
    } answers {
      val isMember = callOriginal()
      check(isMember)
      membershipChecked.countDown()
      check(releaseAddition.await(10, TimeUnit.SECONDS))
      isMember
    }
    val concurrentGroupService =
      GroupServiceDataImpl(
        context.getBean(GroupRepository::class.java),
        context.getBean(GroupWithMemberCountRepository::class.java),
        groupMemberRepository,
        context.getBean(GroupMemberWithUserInfoRepository::class.java),
        concurrentPermissionRepository,
        organizationRepository,
        configurationRepository,
        mappingRepository,
      )
    val concurrentOrganizationRepository = spyk(organizationRepository)
    every {
      concurrentOrganizationRepository.findByIdForUpdate(tenant.organizationId)
    } answers {
      scimOrganizationLockAttempted.countDown()
      callOriginal()
    }
    val cleanupGroupMemberRepository = spyk(groupMemberRepository)
    every {
      cleanupGroupMemberRepository.deleteByUserIdAndOrganizationId(existingUser.id, tenant.organizationId)
    } answers {
      val deleted = callOriginal()
      cleanupReached.countDown()
      check(releaseCleanup.await(10, TimeUnit.SECONDS))
      deleted
    }
    val concurrentLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        userRepository,
        permissionRepository,
        cleanupGroupMemberRepository,
      )
    val concurrentMutationService = ScimMutationService(concurrentOrganizationRepository, configurationRepository, transactions)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val additionFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            transactions.executeWrite {
              concurrentGroupService.addGroupMember(
                GroupId(groupId),
                UserId(existingUser.id),
                OrganizationId(tenant.organizationId),
                io.airbyte.data.services.GroupMembershipSource.Manual,
              )
              Unit
            }
          }
        }
      check(membershipChecked.await(10, TimeUnit.SECONDS))
      val inactivePostFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            concurrentMutationService.execute(tenant.context) {
              concurrentLifecycle.create(
                tenant.configurationId,
                tenant.organizationId,
                input(false, existingUser.email),
              )
            }
            Unit
          }
        }
      check(scimOrganizationLockAttempted.await(10, TimeUnit.SECONDS))

      assertThat(inactivePostFuture.isDone).isFalse()
      releaseAddition.countDown()
      val additionResult = additionFuture.get(30, TimeUnit.SECONDS)
      check(cleanupReached.await(10, TimeUnit.SECONDS))
      releaseCleanup.countDown()
      val inactivePostResult = inactivePostFuture.get(30, TimeUnit.SECONDS)

      assertThat(additionResult.isSuccess).isTrue()
      assertThat(inactivePostResult.isSuccess).isTrue()
      assertThat(directPermissionCount(existingUser.id, tenant.organizationId)).isZero()
      assertThat(groupMembershipCount(existingUser.id, groupId)).isZero()
      assertThat(mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId).single().userActive).isFalse()
    } finally {
      releaseAddition.countDown()
      releaseCleanup.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent permission grant cannot restore access after deactivation commits`() {
    val tenant = tenant("permission-deactivation-race")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }
    val workspaceId = workspace(tenant.organizationId, "permission-deactivation-race-workspace")
    val workspaceService = mockk<WorkspaceService>()
    every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.of(tenant.organizationId)
    val grantLockedMapping = CountDownLatch(1)
    val releaseGrant = CountDownLatch(1)
    val concurrentMappingRepository = spyk(mappingRepository)
    every {
      concurrentMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(created.userId, tenant.organizationId)
    } answers {
      val mapping = callOriginal()
      check(mapping != null)
      grantLockedMapping.countDown()
      check(releaseGrant.await(10, TimeUnit.SECONDS))
      mapping
    }
    val guardedPermissionService =
      PermissionServiceDataImpl(
        workspaceService,
        permissionRepository,
        configurationRepository,
        concurrentMappingRepository,
      )
    val deactivationConfigurationLockAttempted = CountDownLatch(1)
    val concurrentConfigurationRepository = spyk(configurationRepository)
    every {
      concurrentConfigurationRepository.findByIdAndOrganizationIdForUpdate(tenant.configurationId, tenant.organizationId)
    } answers {
      deactivationConfigurationLockAttempted.countDown()
      callOriginal()
    }
    val concurrentMutationService = ScimMutationService(organizationRepository, concurrentConfigurationRepository, transactions)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val grantFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            transactions.executeWrite {
              guardedPermissionService.createPermission(
                io.airbyte.config
                  .Permission()
                  .withPermissionId(UUID.randomUUID())
                  .withUserId(created.userId)
                  .withWorkspaceId(workspaceId)
                  .withPermissionType(io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN),
              )
              Unit
            }
          }
        }
      check(grantLockedMapping.await(10, TimeUnit.SECONDS))
      val deactivationFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            concurrentMutationService.execute(tenant.context) {
              lifecycleService.replace(tenant.configurationId, tenant.organizationId, created.id, input(false))
            }
            Unit
          }
        }
      check(deactivationConfigurationLockAttempted.await(10, TimeUnit.SECONDS))

      assertThat(deactivationFuture.isDone).isFalse()
      releaseGrant.countDown()
      assertThat(grantFuture.get(30, TimeUnit.SECONDS).isSuccess).isTrue()
      assertThat(deactivationFuture.get(30, TimeUnit.SECONDS).isSuccess).isTrue()
      assertThat(workspacePermissionCount(created.userId, workspaceId)).isZero()
    } finally {
      releaseGrant.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent Group addition cannot restore access after deactivation commits`() {
    val tenant = tenant("group-deactivation-race")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }
    val groupId = group(tenant.organizationId, "Concurrent Group")
    val grantLockedMapping = CountDownLatch(1)
    val releaseGrant = CountDownLatch(1)
    val concurrentMappingRepository = spyk(mappingRepository)
    every {
      concurrentMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(created.userId, tenant.organizationId)
    } answers {
      val mapping = callOriginal()
      check(mapping != null)
      grantLockedMapping.countDown()
      check(releaseGrant.await(10, TimeUnit.SECONDS))
      mapping
    }
    val concurrentGroupService =
      GroupServiceDataImpl(
        context.getBean(GroupRepository::class.java),
        context.getBean(GroupWithMemberCountRepository::class.java),
        groupMemberRepository,
        context.getBean(GroupMemberWithUserInfoRepository::class.java),
        permissionRepository,
        organizationRepository,
        configurationRepository,
        concurrentMappingRepository,
      )
    val deactivationOrganizationLockAttempted = CountDownLatch(1)
    val concurrentOrganizationRepository = spyk(organizationRepository)
    every {
      concurrentOrganizationRepository.findByIdForUpdate(tenant.organizationId)
    } answers {
      deactivationOrganizationLockAttempted.countDown()
      callOriginal()
    }
    val concurrentMutationService = ScimMutationService(concurrentOrganizationRepository, configurationRepository, transactions)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val grantFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            transactions.executeWrite {
              concurrentGroupService.addGroupMember(
                GroupId(groupId),
                UserId(created.userId),
                OrganizationId(tenant.organizationId),
                io.airbyte.data.services.GroupMembershipSource.Manual,
              )
              Unit
            }
          }
        }
      check(grantLockedMapping.await(10, TimeUnit.SECONDS))
      val deactivationFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            concurrentMutationService.execute(tenant.context) {
              lifecycleService.replace(tenant.configurationId, tenant.organizationId, created.id, input(false))
            }
            Unit
          }
        }
      check(deactivationOrganizationLockAttempted.await(10, TimeUnit.SECONDS))

      assertThat(deactivationFuture.isDone).isFalse()
      releaseGrant.countDown()
      assertThat(grantFuture.get(30, TimeUnit.SECONDS).isSuccess).isTrue()
      assertThat(deactivationFuture.get(30, TimeUnit.SECONDS).isSuccess).isTrue()
      assertThat(groupMembershipCount(created.userId, groupId)).isZero()
    } finally {
      releaseGrant.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `list filters and paginates in PostgreSQL before batch group enrichment`() {
    val tenant = tenant("list-page")
    listOf("first", "second", "third").forEach { name ->
      mutationService.execute(tenant.context) {
        lifecycleService.create(
          tenant.configurationId,
          tenant.organizationId,
          input(true, "$name@example.com", "external-$name", name),
        )
      }
    }
    val orderedMappings = mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId)
    val selected = orderedMappings[1]
    val group = group(tenant.organizationId, "Selected Group")
    groupMemberRepository.save(GroupMember(groupId = group, userId = selected.userId!!))
    mappingRepository.save(groupMapping(tenant, group, "Selected Group"))
    val otherTenant = tenant("list-page-other")
    mutationService.execute(otherTenant.context) {
      lifecycleService.create(
        otherTenant.configurationId,
        otherTenant.organizationId,
        input(true, selected.primaryEmail!!, "external-other", "Other Tenant User"),
      )
    }
    val otherGroup = group(otherTenant.organizationId, "Other Tenant Group")
    groupMemberRepository.save(GroupMember(groupId = otherGroup, userId = selected.userId!!))
    mappingRepository.save(groupMapping(otherTenant, otherGroup, "Other Tenant Group"))

    val page = lifecycleService.list(tenant.configurationId, tenant.organizationId, offset = 1, limit = 1)
    val enriched = lifecycleService.enrichGroups(tenant.configurationId, tenant.organizationId, page.resources)
    val otherPage = lifecycleService.list(otherTenant.configurationId, otherTenant.organizationId, offset = 0, limit = 1)
    val otherEnriched = lifecycleService.enrichGroups(otherTenant.configurationId, otherTenant.organizationId, otherPage.resources)
    val filtered =
      lifecycleService.list(
        tenant.configurationId,
        tenant.organizationId,
        listOf(
          ScimUserFilterClause(
            ScimUserFilterAttribute.USER_NAME,
            selected.userName!!.uppercase(),
          ),
          ScimUserFilterClause(
            ScimUserFilterAttribute.EXTERNAL_ID,
            selected.externalId!!,
          ),
          ScimUserFilterClause(
            ScimUserFilterAttribute.WORK_EMAIL,
            selected.primaryEmail!!.uppercase(),
          ),
        ),
        offset = 0,
        limit = 1,
      )

    assertThat(page.totalResults).isEqualTo(3)
    assertThat(page.resources).extracting<UUID> { it.id }.containsExactly(selected.id)
    assertThat(enriched.single().groups.map { it.displayName }).containsExactly("Selected Group")
    assertThat(otherPage.totalResults).isEqualTo(1)
    assertThat(otherEnriched.single().groups.map { it.displayName }).containsExactly("Other Tenant Group")
    assertThat(filtered.totalResults).isEqualTo(1)
    assertThat(filtered.resources).extracting<UUID> { it.id }.containsExactly(selected.id)
  }

  @Test
  fun `list combines repeated multi valued clauses and short circuits contradictory scalar clauses`() {
    val tenant = tenant("list-repeated-clauses")
    val userInput =
      input(
        active = true,
        userName = "primary@example.com",
        externalId = "external-primary",
      ).copy(
        attributes =
          objectMapper.createObjectNode().also {
            val emails = it.putArray("emails")
            emails
              .addObject()
              .put("value", "primary@example.com")
              .put("type", "work")
              .put("primary", true)
            emails
              .addObject()
              .put("value", "alias@example.com")
              .put("type", "home")
            emails
              .addObject()
              .put("value", "backup-work@example.com")
              .put("type", "work")
          },
      )
    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, userInput)
      }
    val persisted = mappingRepository.findUser(created.id, tenant.configurationId, tenant.organizationId)!!

    assertThat(persisted.attributes.path("emails").map { it.path("value").asText() }).containsExactly(
      "primary@example.com",
      "alias@example.com",
      "backup-work@example.com",
    )
    assertThat(
      mappingRepository.countUsers(
        tenant.configurationId,
        tenant.organizationId,
        null,
        null,
        listOf("PRIMARY@EXAMPLE.COM"),
        emptyList(),
      ),
    ).isEqualTo(1)
    assertThat(
      mappingRepository.countUsers(
        tenant.configurationId,
        tenant.organizationId,
        null,
        null,
        listOf("PRIMARY@EXAMPLE.COM", "ALIAS@EXAMPLE.COM"),
        emptyList(),
      ),
    ).isEqualTo(1)

    val emailMatch =
      lifecycleService.list(
        tenant.configurationId,
        tenant.organizationId,
        listOf(
          ScimUserFilterClause(ScimUserFilterAttribute.EMAIL, "PRIMARY@EXAMPLE.COM"),
          ScimUserFilterClause(ScimUserFilterAttribute.EMAIL, "ALIAS@EXAMPLE.COM"),
        ),
        offset = 0,
        limit = 1,
      )
    val workEmailMatch =
      lifecycleService.list(
        tenant.configurationId,
        tenant.organizationId,
        listOf(
          ScimUserFilterClause(ScimUserFilterAttribute.WORK_EMAIL, "PRIMARY@EXAMPLE.COM"),
          ScimUserFilterClause(ScimUserFilterAttribute.WORK_EMAIL, "BACKUP-WORK@EXAMPLE.COM"),
        ),
        offset = 0,
        limit = 1,
      )
    val contradictoryUserName =
      lifecycleService.list(
        tenant.configurationId,
        tenant.organizationId,
        listOf(
          ScimUserFilterClause(ScimUserFilterAttribute.USER_NAME, "PRIMARY@EXAMPLE.COM"),
          ScimUserFilterClause(ScimUserFilterAttribute.USER_NAME, "different@example.com"),
        ),
        offset = 0,
        limit = 1,
      )
    val contradictoryExternalId =
      lifecycleService.list(
        tenant.configurationId,
        tenant.organizationId,
        listOf(
          ScimUserFilterClause(ScimUserFilterAttribute.EXTERNAL_ID, "external-primary"),
          ScimUserFilterClause(ScimUserFilterAttribute.EXTERNAL_ID, "external-different"),
        ),
        offset = 0,
        limit = 1,
      )

    assertThat(emailMatch.totalResults).isEqualTo(1)
    assertThat(emailMatch.resources).extracting<UUID> { it.id }.containsExactly(created.id)
    assertThat(workEmailMatch.totalResults).isEqualTo(1)
    assertThat(workEmailMatch.resources).extracting<UUID> { it.id }.containsExactly(created.id)
    assertThat(contradictoryUserName.totalResults).isZero()
    assertThat(contradictoryUserName.resources).isEmpty()
    assertThat(contradictoryExternalId.totalResults).isZero()
    assertThat(contradictoryExternalId.resources).isEmpty()
  }

  @Test
  fun `failed mutation rolls back user mapping and access together`() {
    val tenant = tenant("rollback")

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true))
        throw ExpectedFailure()
      }
    }.isInstanceOf(ExpectedFailure::class.java)

    assertThat(mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId)).isEmpty()
    assertThat(jooq.fetchCount(Tables.USER)).isZero()
    assertThat(jooq.fetchCount(Tables.PERMISSION)).isZero()
  }

  @Test
  fun `failed PUT rolls back an existing mapping and all scoped access and propagates the error`() {
    val tenant = tenant("put-rollback")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }
    val workspace = workspace(tenant.organizationId, "put-rollback-workspace")
    val group = group(tenant.organizationId, "PUT Rollback Engineering")
    jooq
      .update(Tables.PERMISSION)
      .set(Tables.PERMISSION.PERMISSION_TYPE, PermissionType.organization_admin)
      .where(
        Tables.PERMISSION.USER_ID
          .eq(created.userId)
          .and(Tables.PERMISSION.ORGANIZATION_ID.eq(tenant.organizationId)),
      ).execute()
    permissionRepository.save(Permission(userId = created.userId, workspaceId = workspace, permissionType = PermissionType.workspace_admin))
    groupMemberRepository.save(GroupMember(groupId = group, userId = created.userId))
    mappingRepository.save(groupMapping(tenant, group, "PUT Rollback Engineering"))

    val failure = ExpectedFailure()
    val failingGroupMemberRepository = spyk(groupMemberRepository)
    every { failingGroupMemberRepository.deleteByUserIdAndOrganizationId(created.userId, tenant.organizationId) } throws failure
    val failingLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        userRepository,
        permissionRepository,
        failingGroupMemberRepository,
      )

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        failingLifecycle.replace(
          tenant.configurationId,
          tenant.organizationId,
          created.id,
          input(false, "put-updated@example.com", "put-updated-external", "PUT Updated User"),
        )
      }
    }.isSameAs(failure)

    val restored = lifecycleService.get(tenant.configurationId, tenant.organizationId, created.id)
    assertThat(restored.userName).isEqualTo("shared@example.com")
    assertThat(restored.externalId).isEqualTo("external-shared")
    assertThat(restored.active).isTrue()
    assertThat(restored.attributes.path("displayName").asText()).isEqualTo("Shared User")
    assertThat(
      permissionRepository
        .findByUserId(created.userId)
        .single { it.organizationId == tenant.organizationId }
        .permissionType,
    ).isEqualTo(PermissionType.organization_admin)
    assertThat(workspacePermissionCount(created.userId, workspace)).isEqualTo(1)
    assertThat(groupMembershipCount(created.userId, group)).isEqualTo(1)
    assertThat(restored.groups.map { it.displayName }).containsExactly("PUT Rollback Engineering")
  }

  @Test
  fun `failed multi-operation PATCH rolls back profile cleanup and baseline access and propagates the error`() {
    val tenant = tenant("patch-rollback")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }
    val workspace = workspace(tenant.organizationId, "patch-rollback-workspace")
    val group = group(tenant.organizationId, "PATCH Rollback Engineering")
    jooq
      .update(Tables.PERMISSION)
      .set(Tables.PERMISSION.PERMISSION_TYPE, PermissionType.organization_admin)
      .where(
        Tables.PERMISSION.USER_ID
          .eq(created.userId)
          .and(Tables.PERMISSION.ORGANIZATION_ID.eq(tenant.organizationId)),
      ).execute()
    permissionRepository.save(Permission(userId = created.userId, workspaceId = workspace, permissionType = PermissionType.workspace_admin))
    groupMemberRepository.save(GroupMember(groupId = group, userId = created.userId))
    mappingRepository.save(groupMapping(tenant, group, "PATCH Rollback Engineering"))

    val failure = ExpectedFailure()
    val failingPermissionRepository = spyk(permissionRepository)
    var directCleanupAttempts = 0
    every { failingPermissionRepository.deleteByUserIdAndOrganizationId(created.userId, tenant.organizationId) } answers {
      directCleanupAttempts += 1
      if (directCleanupAttempts == 2) throw failure
      callOriginal()
    }
    val failingLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        userRepository,
        failingPermissionRepository,
        groupMemberRepository,
      )

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        failingLifecycle.patch(
          tenant.configurationId,
          tenant.organizationId,
          created.id,
          input(false, "patch-updated@example.com", "patch-updated-external", "PATCH Updated User"),
          listOf(false, true, false),
        )
      }
    }.isSameAs(failure)

    val restored = lifecycleService.get(tenant.configurationId, tenant.organizationId, created.id)
    assertThat(restored.userName).isEqualTo("shared@example.com")
    assertThat(restored.externalId).isEqualTo("external-shared")
    assertThat(restored.active).isTrue()
    assertThat(restored.attributes.path("displayName").asText()).isEqualTo("Shared User")
    assertThat(
      permissionRepository
        .findByUserId(created.userId)
        .single { it.organizationId == tenant.organizationId }
        .permissionType,
    ).isEqualTo(PermissionType.organization_admin)
    assertThat(workspacePermissionCount(created.userId, workspace)).isEqualTo(1)
    assertThat(groupMembershipCount(created.userId, group)).isEqualTo(1)
    assertThat(restored.groups.map { it.displayName }).containsExactly("PATCH Rollback Engineering")
  }

  @Test
  fun `inactive POST cleans only preexisting target organization access`() {
    val tenantA = tenant("inactive-a")
    val tenantB = tenant("inactive-b")
    val createdB = mutationService.execute(tenantB.context) { lifecycleService.create(tenantB.configurationId, tenantB.organizationId, input(true)) }
    val workspaceA = workspace(tenantA.organizationId, "inactive-workspace-a")
    val workspaceB = workspace(tenantB.organizationId, "inactive-workspace-b")
    val groupA = group(tenantA.organizationId, "Inactive Engineering A")
    val groupB = group(tenantB.organizationId, "Inactive Engineering B")
    permissionRepository.save(
      Permission(
        userId = createdB.userId,
        organizationId = tenantA.organizationId,
        permissionType = PermissionType.organization_admin,
      ),
    )
    permissionRepository.save(Permission(userId = createdB.userId, workspaceId = workspaceA, permissionType = PermissionType.workspace_admin))
    permissionRepository.save(Permission(userId = createdB.userId, workspaceId = workspaceB, permissionType = PermissionType.workspace_admin))
    groupMemberRepository.save(GroupMember(groupId = groupA, userId = createdB.userId))
    groupMemberRepository.save(GroupMember(groupId = groupB, userId = createdB.userId))

    val createdA =
      mutationService.execute(tenantA.context) {
        lifecycleService.create(tenantA.configurationId, tenantA.organizationId, input(false))
      }

    assertThat(createdA.userId).isEqualTo(createdB.userId)
    assertThat(createdA.active).isFalse()
    assertThat(directPermissionCount(createdA.userId, tenantA.organizationId)).isZero()
    assertThat(workspacePermissionCount(createdA.userId, workspaceA)).isZero()
    assertThat(groupMembershipCount(createdA.userId, groupA)).isZero()
    assertThat(directPermissionCount(createdA.userId, tenantB.organizationId)).isEqualTo(1)
    assertThat(workspacePermissionCount(createdA.userId, workspaceB)).isEqualTo(1)
    assertThat(groupMembershipCount(createdA.userId, groupB)).isEqualTo(1)
  }

  @Test
  fun `concurrent two organization POSTs reuse one global User`() {
    val tenantA = tenant("concurrent-a")
    val tenantB = tenant("concurrent-b")
    val ready = CountDownLatch(2)
    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val futures =
        listOf(tenantA, tenantB).map { tenant ->
          executor.submit<UUID> {
            ready.countDown()
            check(start.await(10, TimeUnit.SECONDS))
            mutationService.execute(tenant.context) {
              lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)).userId
            }
          }
        }
      check(ready.await(10, TimeUnit.SECONDS))
      start.countDown()
      val userIds = futures.map { it.get(30, TimeUnit.SECONDS) }

      assertThat(userIds.distinct()).hasSize(1)
      assertThat(jooq.fetchCount(Tables.USER, Tables.USER.EMAIL.equalIgnoreCase("shared@example.com"))).isEqualTo(1)
      assertThat(mappingRepository.findAllUsers(tenantA.configurationId, tenantA.organizationId)).hasSize(1)
      assertThat(mappingRepository.findAllUsers(tenantB.configurationId, tenantB.organizationId)).hasSize(1)
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  fun `oversized profile seed is safely bounded while mappings preserve complete values for new and reused global Users`() {
    val tenantA = tenant("long-profile-a")
    val tenantB = tenant("long-profile-b")
    val email = "long-profile@example.com"
    val firstFormattedName = "n".repeat(255) + "😀" + "tail"
    val secondFormattedName = "Second ".repeat(50)

    val createdA =
      mutationService.execute(tenantA.context) {
        lifecycleService.create(
          tenantA.configurationId,
          tenantA.organizationId,
          inputWithFormattedName(email, firstFormattedName),
        )
      }
    val createdB =
      mutationService.execute(tenantB.context) {
        lifecycleService.create(
          tenantB.configurationId,
          tenantB.organizationId,
          inputWithFormattedName(email, secondFormattedName),
        )
      }

    assertThat(createdB.userId).isEqualTo(createdA.userId)
    assertThat(userRepository.findById(createdA.userId).orElseThrow().name).isEqualTo("n".repeat(255) + "😀")
    assertThat(
      createdA.attributes
        .path("name")
        .path("formatted")
        .asText(),
    ).isEqualTo(firstFormattedName)
    assertThat(
      createdB.attributes
        .path("name")
        .path("formatted")
        .asText(),
    ).isEqualTo(secondFormattedName)
  }

  @Test
  fun `concurrent duplicate POSTs in one configuration yield one mapping and one conflict`() {
    val tenant = tenant("concurrent-duplicate")
    val ready = CountDownLatch(2)
    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val futures =
        (1..2).map {
          executor.submit<Result<UUID>> {
            ready.countDown()
            check(start.await(10, TimeUnit.SECONDS))
            runCatching {
              mutationService.execute(tenant.context) {
                lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)).id
              }
            }
          }
        }
      check(ready.await(10, TimeUnit.SECONDS))
      start.countDown()
      val results = futures.map { it.get(30, TimeUnit.SECONDS) }

      assertThat(results.count { it.isSuccess }).isEqualTo(1)
      assertThat(results.single { it.isFailure }.exceptionOrNull()).isInstanceOf(ScimUserConflictException::class.java)
      assertThat(jooq.fetchCount(Tables.USER, Tables.USER.EMAIL.equalIgnoreCase("shared@example.com"))).isEqualTo(1)
      assertThat(mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId)).hasSize(1)
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent cross organization POST serializes update identity resolution and leaves a conflicting target unchanged`() {
    val tenantA = tenant("concurrent-update-a")
    val tenantB = tenant("concurrent-update-b")
    val target =
      mutationService.execute(tenantA.context) {
        lifecycleService.create(
          tenantA.configurationId,
          tenantA.organizationId,
          input(true, "target@example.com", "target-external", "Target User"),
        )
      }
    val targetBefore = lifecycleService.get(tenantA.configurationId, tenantA.organizationId, target.id)
    val createReachedLookup = CountDownLatch(1)
    val releaseCreateLookup = CountDownLatch(1)
    val updateReachedEmailLock = CountDownLatch(1)
    val lockAttempts = AtomicInteger()
    val lookupAttempts = AtomicInteger()
    val concurrentUserRepository = spyk(userRepository)
    every { concurrentUserRepository.acquireGlobalEmailLock("contended@example.com") } answers {
      if (lockAttempts.incrementAndGet() == 2) {
        updateReachedEmailLock.countDown()
      }
      callOriginal()
    }
    every { concurrentUserRepository.findByEmailIgnoreCaseForUpdate("contended@example.com") } answers {
      if (lookupAttempts.incrementAndGet() == 1) {
        createReachedLookup.countDown()
        check(releaseCreateLookup.await(10, TimeUnit.SECONDS))
      }
      callOriginal()
    }
    val concurrentLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        concurrentUserRepository,
        permissionRepository,
        groupMemberRepository,
      )
    val executor = Executors.newFixedThreadPool(2)

    try {
      val createFuture =
        executor.submit<Result<UUID>> {
          runCatching {
            mutationService.execute(tenantB.context) {
              concurrentLifecycle
                .create(
                  tenantB.configurationId,
                  tenantB.organizationId,
                  input(true, "contended@example.com", "contended-external", "Contended User"),
                ).userId
            }
          }
        }
      check(createReachedLookup.await(10, TimeUnit.SECONDS))

      val updateFuture =
        executor.submit<Result<UUID>> {
          runCatching {
            mutationService.execute(tenantA.context) {
              concurrentLifecycle
                .replace(
                  tenantA.configurationId,
                  tenantA.organizationId,
                  target.id,
                  input(true, "contended@example.com", "updated-external", "Updated User"),
                ).userId
            }
          }
        }

      val updateWasSerialized = updateReachedEmailLock.await(5, TimeUnit.SECONDS)
      val updateCompletedWhileCreateHeldIdentityLock = updateFuture.isDone
      releaseCreateLookup.countDown()
      val createResult = createFuture.get(30, TimeUnit.SECONDS)
      val updateResult = updateFuture.get(30, TimeUnit.SECONDS)

      assertThat(updateWasSerialized).isTrue()
      assertThat(updateCompletedWhileCreateHeldIdentityLock).isFalse()
      assertThat(createResult.isSuccess).isTrue()
      assertThat(updateResult.exceptionOrNull()).isInstanceOf(ScimUserConflictException::class.java)

      val targetAfter = lifecycleService.get(tenantA.configurationId, tenantA.organizationId, target.id)
      val createdB = mappingRepository.findAllUsers(tenantB.configurationId, tenantB.organizationId).single()
      assertThat(targetAfter).isEqualTo(targetBefore)
      assertThat(createdB.primaryEmail).isEqualTo("contended@example.com")
      assertThat(createdB.userId).isEqualTo(createResult.getOrThrow())
      assertThat(createdB.userId).isNotEqualTo(target.userId)
      assertThat(jooq.fetchCount(Tables.USER, Tables.USER.EMAIL.equalIgnoreCase("contended@example.com"))).isEqualTo(1)
    } finally {
      releaseCreateLookup.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `SCIM POST serializes with ordinary global User creation`() {
    val tenant = tenant("ordinary-create-race")
    val email = "ordinary-create-race@example.com"
    val lookupReached = CountDownLatch(1)
    val releaseLookup = CountDownLatch(1)
    val ordinaryStarted = CountDownLatch(1)
    val ordinaryFinished = CountDownLatch(1)
    val concurrentUserRepository = spyk(userRepository)
    every { concurrentUserRepository.findByEmailIgnoreCaseForUpdate(email) } answers {
      lookupReached.countDown()
      check(releaseLookup.await(10, TimeUnit.SECONDS))
      callOriginal()
    }
    val concurrentLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        concurrentUserRepository,
        permissionRepository,
        groupMemberRepository,
      )
    val ordinaryUser = ordinaryUser(email)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val scimFuture =
        executor.submit<Result<UUID>> {
          runCatching {
            mutationService.execute(tenant.context) {
              concurrentLifecycle
                .create(
                  tenant.configurationId,
                  tenant.organizationId,
                  input(true, email, "ordinary-create-race", "SCIM User"),
                ).userId
            }
          }
        }
      check(lookupReached.await(10, TimeUnit.SECONDS))
      val ordinaryFuture =
        executor.submit<Result<Unit>> {
          ordinaryStarted.countDown()
          runCatching { userPersistence.writeUser(ordinaryUser) }
            .also { ordinaryFinished.countDown() }
        }
      check(ordinaryStarted.await(10, TimeUnit.SECONDS))

      val ordinaryCompletedWhileScimHeldEmailLock = ordinaryFinished.await(2, TimeUnit.SECONDS)
      releaseLookup.countDown()
      val scimResult = scimFuture.get(30, TimeUnit.SECONDS)
      val ordinaryResult = ordinaryFuture.get(30, TimeUnit.SECONDS)

      assertThat(ordinaryCompletedWhileScimHeldEmailLock).isFalse()
      assertThat(scimResult.isSuccess).isTrue()
      assertThat(ordinaryResult.isFailure).isTrue()
      val mapping = mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId).single()
      assertThat(mapping.userId).isEqualTo(scimResult.getOrThrow())
      assertThat(jooq.fetchCount(Tables.USER, Tables.USER.EMAIL.equalIgnoreCase(email))).isEqualTo(1)
      assertThat(userRepository.findById(ordinaryUser.userId)).isEmpty
    } finally {
      releaseLookup.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `SCIM PUT serializes with an ordinary global User email update`() {
    val tenant = tenant("ordinary-update-race")
    val target =
      mutationService.execute(tenant.context) {
        lifecycleService.create(
          tenant.configurationId,
          tenant.organizationId,
          input(true, "ordinary-update-target@example.com", "ordinary-update-target", "Target User"),
        )
      }
    val ordinaryUser = ordinaryUser("ordinary-update-source@example.com")
    userPersistence.writeUser(ordinaryUser)
    val contendedEmail = "ordinary-update-race@example.com"
    val lookupReached = CountDownLatch(1)
    val releaseLookup = CountDownLatch(1)
    val ordinaryStarted = CountDownLatch(1)
    val ordinaryFinished = CountDownLatch(1)
    val concurrentUserRepository = spyk(userRepository)
    every { concurrentUserRepository.findByEmailIgnoreCaseForUpdate(contendedEmail) } answers {
      lookupReached.countDown()
      check(releaseLookup.await(10, TimeUnit.SECONDS))
      callOriginal()
    }
    val concurrentLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        concurrentUserRepository,
        permissionRepository,
        groupMemberRepository,
      )
    val executor = Executors.newFixedThreadPool(2)

    try {
      val scimFuture =
        executor.submit<Result<UUID>> {
          runCatching {
            mutationService.execute(tenant.context) {
              concurrentLifecycle
                .replace(
                  tenant.configurationId,
                  tenant.organizationId,
                  target.id,
                  input(true, contendedEmail, "ordinary-update-race", "Updated Target"),
                ).userId
            }
          }
        }
      check(lookupReached.await(10, TimeUnit.SECONDS))
      val ordinaryFuture =
        executor.submit<Result<Unit>> {
          ordinaryStarted.countDown()
          runCatching { userPersistence.writeUser(ordinaryUser.withEmail(contendedEmail)) }
            .also { ordinaryFinished.countDown() }
        }
      check(ordinaryStarted.await(10, TimeUnit.SECONDS))

      val ordinaryCompletedWhileScimHeldEmailLock = ordinaryFinished.await(2, TimeUnit.SECONDS)
      releaseLookup.countDown()
      val scimResult = scimFuture.get(30, TimeUnit.SECONDS)
      val ordinaryResult = ordinaryFuture.get(30, TimeUnit.SECONDS)

      assertThat(ordinaryCompletedWhileScimHeldEmailLock).isFalse()
      assertThat(scimResult.getOrThrow()).isEqualTo(target.userId)
      assertThat(ordinaryResult.isSuccess).isTrue()
      val mapping = mappingRepository.findUser(target.id, tenant.configurationId, tenant.organizationId)!!
      assertThat(mapping.primaryEmail).isEqualTo(contendedEmail)
      assertThat(
        jooq
          .select(Tables.USER.EMAIL)
          .from(Tables.USER)
          .where(Tables.USER.ID.eq(ordinaryUser.userId))
          .fetchOne(Tables.USER.EMAIL),
      ).isEqualTo(contendedEmail)
    } finally {
      releaseLookup.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `DELETE makes the old id unreachable and later POST creates a new mapping id`() {
    val tenant = tenant("delete-recreate")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }

    mutationService.execute(tenant.context) {
      lifecycleService.delete(tenant.configurationId, tenant.organizationId, created.id)
    }
    assertThatThrownBy { lifecycleService.get(tenant.configurationId, tenant.organizationId, created.id) }
      .isInstanceOf(ScimUserNotFoundException::class.java)

    val recreated = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }

    assertThat(recreated.id).isNotEqualTo(created.id)
    assertThat(recreated.userId).isEqualTo(created.userId)
  }

  @Test
  fun `contract-invalid profile URLs on POST PUT and PATCH make zero database writes`() {
    val tenant = tenant("invalid-profile-url")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }

    assertRejectedWithoutWrites(tenant) {
      createAndRender(
        tenant,
        rawUser("profile-post@example.com", "\"profileUrl\":\"https://example .com/post\","),
      )
    }
    assertRejectedWithoutWrites(tenant) {
      replaceAndRender(
        tenant,
        created.id,
        rawUser("shared@example.com", "\"profileUrl\":\"https://example .com/put\","),
      )
    }
    assertRejectedWithoutWrites(tenant) {
      patchAndRender(
        tenant,
        created.id,
        """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"profileUrl","value":"https://example .com/patch"}]}""",
      )
    }
    assertRejectedWithoutWrites(tenant) {
      createAndRender(
        tenant,
        rawUser("profile-urn@example.com", "\"profileUrl\":\"urn:example:user:alice\","),
      )
    }
    assertRejectedWithoutWrites(tenant) {
      replaceAndRender(
        tenant,
        created.id,
        rawUser("shared@example.com", "\"profileUrl\":\"file:///profiles/alice\","),
      )
    }
    assertRejectedWithoutWrites(tenant) {
      patchAndRender(
        tenant,
        created.id,
        """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"profileUrl","value":"https:profile"}]}""",
      )
    }
  }

  @Test
  fun `malformed email values on POST PUT and PATCH make zero database writes`() {
    val tenant = tenant("invalid-email-values")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }
    val oversizedEmail = "${"l".repeat(64)}@${"d".repeat(63)}.${"e".repeat(63)}.${"f".repeat(62)}"

    assertRejectedWithoutWrites(tenant) {
      createAndRender(
        tenant,
        """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"invalid-post@example.com","emails":[{"value":"valid-post@example.com","type":"work","primary":true},{"value":"alice@.example.com","type":"home"}]}""",
      )
    }
    assertRejectedWithoutWrites(tenant) {
      replaceAndRender(
        tenant,
        created.id,
        """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"invalid-put@example.com","emails":[{"value":"valid-put@example.com","type":"work","primary":true},{"value":".alice@example.com","type":"home"}]}""",
      )
    }
    assertRejectedWithoutWrites(tenant) {
      patchAndRender(
        tenant,
        created.id,
        """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"emails","value":[{"value":"valid-patch@example.com","type":"work","primary":true},{"value":"alice@-example.com","type":"home"}]}]}""",
      )
    }
    assertRejectedWithoutWrites(tenant) {
      createAndRender(
        tenant,
        """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"oversized@example.com","emails":[{"value":"$oversizedEmail","type":"work"}]}""",
      )
    }
    userRepository.save(ScimAirbyteUser(name = "Existing", email = oversizedEmail))
    assertRejectedWithoutWrites(tenant) {
      createAndRender(
        tenant,
        """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"oversized-reused@example.com","emails":[{"value":"$oversizedEmail","type":"work"}]}""",
      )
    }
  }

  @Test
  fun `duplicate email values on POST PUT and PATCH make zero database writes`() {
    val tenant = tenant("duplicate-email-values")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }

    assertRejectedWithoutWrites(tenant) {
      createAndRender(tenant, rawUserWithDuplicateEmails("duplicate-post@example.com"))
    }
    assertRejectedWithoutWrites(tenant) {
      replaceAndRender(tenant, created.id, rawUserWithDuplicateEmails("shared@example.com"))
    }
    assertRejectedWithoutWrites(tenant) {
      patchAndRender(
        tenant,
        created.id,
        """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"emails","value":[{"value":"patch@example.com","type":"work"},{"value":" PATCH@EXAMPLE.COM ","type":"home"}]}]}""",
      )
    }
  }

  @Test
  fun `PATCH direct and pathless email add and replace discard display before PostgreSQL writes`() {
    val operations =
      listOf(
        """{"op":"add","path":"emails","value":{"value":"direct-add@example.com","type":"home","display":"Direct add"}}""",
        """{"op":"replace","path":"emails","value":[{"value":"direct-replace@example.com","type":"work","display":"Direct replace"}]}""",
        """{"op":"add","value":{"emails":[{"value":"pathless-add@example.com","type":"home","display":"Pathless add"}]}}""",
        """{"op":"replace","value":{"emails":[{"value":"pathless-replace@example.com","type":"work","display":"Pathless replace"}]}}""",
      )

    operations.forEachIndexed { index, operation ->
      val tenant = tenant("patch-display-$index")
      val userName = "patch-display-$index@example.com"
      val created =
        mutationService.execute(tenant.context) {
          lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true, userName))
        }

      patchAndRender(
        tenant,
        created.id,
        """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[$operation]}""",
      )

      val persisted = lifecycleService.get(tenant.configurationId, tenant.organizationId, created.id)
      assertThat(persisted.attributes.path("emails")).allSatisfy { email -> assertThat(email.has("display")).isFalse() }
    }
  }

  @Test
  fun `PATCH non-string email display in direct and pathless add and replace makes zero PostgreSQL writes`() {
    val tenant = tenant("patch-invalid-display")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }
    listOf(
      """{"op":"add","path":"emails","value":{"value":"direct-add@example.com","type":"home","display":false}}""",
      """{"op":"replace","path":"emails","value":[{"value":"direct-replace@example.com","type":"work","display":false}]}""",
      """{"op":"add","value":{"emails":[{"value":"pathless-add@example.com","type":"home","display":false}]}}""",
      """{"op":"replace","value":{"emails":[{"value":"pathless-replace@example.com","type":"work","display":false}]}}""",
    ).forEach { operation ->
      assertRejectedWithoutWrites(tenant) {
        patchAndRender(
          tenant,
          created.id,
          """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[$operation]}""",
        )
      }
    }
  }

  @Test
  fun `PATCH duplicate case-insensitive pathless attributes and subattributes make zero PostgreSQL writes`() {
    val tenant = tenant("patch-duplicate-fields")
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true)) }
    listOf(
      """{"op":"add","value":{"userName":"first@example.com","USERNAME":"second@example.com"}}""",
      """{"op":"replace","value":{"displayName":"First","DISPLAYNAME":"Second"}}""",
      """{"op":"add","path":"name","value":{"givenName":"First","GIVENNAME":"Second"}}""",
      """{"op":"replace","value":{"name":{"familyName":"First","FAMILYNAME":"Second"}}}""",
      """{"op":"add","path":"emails","value":{"value":"first@example.com","VALUE":"second@example.com","type":"home"}}""",
      """{"op":"replace","value":{"emails":[{"value":"first@example.com","VALUE":"second@example.com","type":"work"}]}}""",
    ).forEach { operation ->
      assertRejectedWithoutWrites(tenant) {
        patchAndRender(
          tenant,
          created.id,
          """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[$operation]}""",
        )
      }
    }
  }

  @Test
  fun `repository failure rolls back global User and mapping and propagates the error`() {
    val tenant = tenant("repository-rollback")
    val failingPermissionRepository = mockk<PermissionRepository>()
    val failure = ExpectedFailure()
    every { failingPermissionRepository.existsByUserIdAndOrganizationId(any(), tenant.organizationId) } returns false
    every { failingPermissionRepository.save(any()) } throws failure
    val failingLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        userRepository,
        failingPermissionRepository,
        groupMemberRepository,
      )

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        failingLifecycle.create(tenant.configurationId, tenant.organizationId, input(true))
      }
    }.isSameAs(failure)

    assertThat(mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId)).isEmpty()
    assertThat(jooq.fetchCount(Tables.USER)).isZero()
  }

  private fun assertRejectedWithoutWrites(
    tenant: Tenant,
    action: () -> Unit,
  ) {
    val mappingsBefore = mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId)
    val usersBefore = jooq.fetchCount(Tables.USER)
    val permissionsBefore = jooq.fetchCount(Tables.PERMISSION)

    val thrown = runCatching(action).exceptionOrNull()

    assertThat(thrown).isInstanceOf(ScimException::class.java)
    assertThat((thrown as ScimException).scimType).isEqualTo("invalidValue")
    assertThat(mappingRepository.findAllUsers(tenant.configurationId, tenant.organizationId)).isEqualTo(mappingsBefore)
    assertThat(jooq.fetchCount(Tables.USER)).isEqualTo(usersBefore)
    assertThat(jooq.fetchCount(Tables.PERMISSION)).isEqualTo(permissionsBefore)
  }

  private fun createAndRender(
    tenant: Tenant,
    body: String,
  ) {
    val input = ScimUserRequestParser.parse(objectMapper.readTree(body) as ObjectNode)
    val created = mutationService.execute(tenant.context) { lifecycleService.create(tenant.configurationId, tenant.organizationId, input) }
    ScimUserResourceService(objectMapper).render(created, URI.create("https://example.com/"), null, null)
  }

  private fun replaceAndRender(
    tenant: Tenant,
    id: UUID,
    body: String,
  ) {
    val input = ScimUserRequestParser.parse(objectMapper.readTree(body) as ObjectNode)
    val replaced = mutationService.execute(tenant.context) { lifecycleService.replace(tenant.configurationId, tenant.organizationId, id, input) }
    ScimUserResourceService(objectMapper).render(replaced, URI.create("https://example.com/"), null, null)
  }

  private fun patchAndRender(
    tenant: Tenant,
    id: UUID,
    body: String,
  ) {
    val resourceService = ScimUserResourceService(objectMapper)
    val patched =
      mutationService.execute(tenant.context) {
        val current = lifecycleService.get(tenant.configurationId, tenant.organizationId, id)
        val patch =
          ScimPatchProcessor.applyUser(
            resourceService.completeResource(current, URI.create("https://example.com/")),
            objectMapper.readTree(body) as ObjectNode,
          )
        lifecycleService.patch(
          tenant.configurationId,
          tenant.organizationId,
          id,
          ScimUserRequestParser.parse(patch.resource),
          patch.activeTransitions.map { it.to },
        )
      }
    resourceService.render(patched, URI.create("https://example.com/"), null, null)
  }

  private fun rawUser(
    email: String,
    extraFields: String = "",
  ): String = """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"$email",$extraFields"emails":[{"value":"$email","type":"work"}]}"""

  private fun rawUserWithDuplicateEmails(email: String): String =
    """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"$email","emails":[{"value":"$email","type":"work"},{"value":" ${email.uppercase()} ","type":"home"}]}"""

  private fun ordinaryUser(email: String): User =
    User()
      .withUserId(UUID.randomUUID())
      .withName("Ordinary User")
      .withEmail(email)
      .withNews(false)
      .withUiMetadata(objectMapper.createObjectNode())

  private fun tenant(name: String): Tenant {
    val organization = organizationRepository.save(Organization(name = name, email = "$name@example.com"))
    val tokenHash =
      UUID
        .randomUUID()
        .toString()
        .replace("-", "")
        .repeat(2)
    val configuration =
      configurationRepository.save(
        ScimConfiguration(
          organizationId = organization.id!!,
          tokenHash = tokenHash,
          idpProvider = "okta",
          enabled = true,
          tokenIssuedAt = OffsetDateTime.now(),
        ),
      )
    return Tenant(
      organizationId = organization.id!!,
      configurationId = configuration.id!!,
      context = ScimAuthenticationContext(configuration.id!!, OrganizationId(organization.id!!), tokenHash),
    )
  }

  private fun input(
    active: Boolean,
    userName: String = "shared@example.com",
    externalId: String = "external-shared",
    displayName: String = "Shared User",
  ): ScimUserWrite =
    ScimUserWrite(
      userName = userName,
      externalId = externalId,
      primaryEmail = userName,
      active = active,
      attributes =
        objectMapper.createObjectNode().also {
          it.put("displayName", displayName)
          it
            .putArray("emails")
            .addObject()
            .put("value", userName)
            .put("type", "work")
            .put("primary", true)
        },
    )

  private fun inputWithFormattedName(
    email: String,
    formattedName: String,
  ): ScimUserWrite =
    input(active = true, userName = email).copy(
      attributes =
        objectMapper.createObjectNode().also {
          it.putObject("name").put("formatted", formattedName)
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
    jooq
      .insertInto(Tables.DATAPLANE_GROUP)
      .set(Tables.DATAPLANE_GROUP.ID, dataplaneGroupId)
      .set(Tables.DATAPLANE_GROUP.ORGANIZATION_ID, organizationId)
      .set(Tables.DATAPLANE_GROUP.NAME, "$name dataplane")
      .execute()
    return UUID.randomUUID().also { workspaceId ->
      jooq
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
      jooq
        .insertInto(Tables.GROUP)
        .set(Tables.GROUP.ID, groupId)
        .set(Tables.GROUP.NAME, name)
        .set(Tables.GROUP.ORGANIZATION_ID, organizationId)
        .execute()
    }

  private fun groupMapping(
    tenant: Tenant,
    groupId: UUID,
    name: String,
  ): ScimResourceMapping =
    ScimResourceMapping(
      scimConfigurationId = tenant.configurationId,
      organizationId = tenant.organizationId,
      resourceType = ScimResourceType.GROUP,
      groupId = groupId,
      externalId = "group-$name",
      attributes = objectMapper.createObjectNode(),
    )

  private fun directPermissionCount(
    userId: UUID,
    organizationId: UUID,
  ): Int =
    jooq.fetchCount(
      Tables.PERMISSION,
      Tables.PERMISSION.USER_ID
        .eq(userId)
        .and(Tables.PERMISSION.ORGANIZATION_ID.eq(organizationId)),
    )

  private fun workspacePermissionCount(
    userId: UUID,
    workspaceId: UUID,
  ): Int =
    jooq.fetchCount(
      Tables.PERMISSION,
      Tables.PERMISSION.USER_ID
        .eq(userId)
        .and(Tables.PERMISSION.WORKSPACE_ID.eq(workspaceId)),
    )

  private fun groupMembershipCount(
    userId: UUID,
    groupId: UUID,
  ): Int =
    jooq.fetchCount(
      Tables.GROUP_MEMBER,
      Tables.GROUP_MEMBER.USER_ID
        .eq(userId)
        .and(Tables.GROUP_MEMBER.GROUP_ID.eq(groupId)),
    )

  private fun invitation(
    inviterUserId: UUID,
    scopeId: UUID,
    scopeType: ScopeType,
    permissionType: PermissionType,
  ): io.airbyte.data.repositories.entities.UserInvitation =
    userInvitationRepository.save(
      io.airbyte.data.repositories.entities.UserInvitation(
        inviteCode = UUID.randomUUID().toString(),
        inviterUserId = inviterUserId,
        invitedEmail = "shared@example.com",
        scopeId = scopeId,
        scopeType = scopeType,
        permissionType = permissionType,
        status = InvitationStatus.pending,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

  private data class Tenant(
    val organizationId: UUID,
    val configurationId: UUID,
    val context: ScimAuthenticationContext,
  )

  private class ExpectedFailure : RuntimeException()

  companion object {
    private lateinit var context: ApplicationContext
    private lateinit var jooq: DSLContext
    private lateinit var objectMapper: ObjectMapper
    private lateinit var organizationRepository: OrganizationRepository
    private lateinit var configurationRepository: ScimConfigurationRepository
    private lateinit var mappingRepository: ScimResourceMappingRepository
    private lateinit var userRepository: ScimAirbyteUserRepository
    private lateinit var permissionRepository: PermissionRepository
    private lateinit var groupMemberRepository: GroupMemberRepository
    private lateinit var userInvitationRepository: UserInvitationRepository
    private lateinit var applicationService: ApplicationService
    private lateinit var permissionService: PermissionService
    private lateinit var groupService: GroupService
    private lateinit var lifecycleService: ScimUserLifecycleService
    private lateinit var mutationService: ScimMutationService
    private lateinit var userPersistence: UserPersistence
    private lateinit var transactions: TransactionOperations<Connection>
    private lateinit var jwtTokenGenerator: JwtTokenGenerator
    private lateinit var jwtTokenValidator: ReactiveJsonWebTokenValidator<*, HttpRequest<*>>

    private val container: PostgreSQLContainer<*> =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker")

    @BeforeAll
    @JvmStatic
    fun setUpDatabase() {
      container.start()
      context =
        ApplicationContext.run(
          PropertySource.of(
            "scim-user-lifecycle-test",
            mapOf(
              "datasources.config.driverClassName" to "org.postgresql.Driver",
              "datasources.config.db-type" to "postgres",
              "datasources.config.dialect" to "POSTGRES",
              "datasources.config.url" to container.jdbcUrl,
              "datasources.config.username" to container.username,
              "datasources.config.password" to container.password,
              "airbyte.auth.token-issuer" to "http://test-url.com",
              "micronaut.security.enabled" to "true",
              "micronaut.security.token.jwt.enabled" to "true",
              "micronaut.security.token.jwt.signatures.secret.generator.secret" to
                "test-jwt-signature-secret-that-is-long-enough-for-hs256",
            ),
          ),
        )
      val dataSource =
        (context.getBean(DataSource::class.java, Qualifiers.byName("config")) as DelegatingDataSource)
          .targetDataSource
      jooq = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
      val database = TestDatabaseProviders(dataSource, jooq).createNewConfigsDatabase()
      userPersistence = UserPersistence(database)

      objectMapper = context.getBean(ObjectMapper::class.java)
      organizationRepository = context.getBean(OrganizationRepository::class.java)
      configurationRepository = context.getBean(ScimConfigurationRepository::class.java)
      mappingRepository = context.getBean(ScimResourceMappingRepository::class.java)
      userRepository = context.getBean(ScimAirbyteUserRepository::class.java)
      permissionRepository = context.getBean(PermissionRepository::class.java)
      groupMemberRepository = context.getBean(GroupMemberRepository::class.java)
      userInvitationRepository = context.getBean(UserInvitationRepository::class.java)
      permissionService = context.getBean(PermissionService::class.java)
      groupService = context.getBean(GroupService::class.java)
      jwtTokenGenerator = context.getBean(JwtTokenGenerator::class.java)
      applicationService =
        ApplicationServiceDataImpl(
          context.getBean(ApplicationRepository::class.java),
          context.getBean(AirbyteAuthConfig::class.java),
          jwtTokenGenerator,
        )
      @Suppress("UNCHECKED_CAST")
      jwtTokenValidator =
        context.getBean(ReactiveJsonWebTokenValidator::class.java) as ReactiveJsonWebTokenValidator<*, HttpRequest<*>>
      @Suppress("UNCHECKED_CAST")
      transactions =
        context.getBean(TransactionOperations::class.java, Qualifiers.byName("config")) as TransactionOperations<Connection>
      lifecycleService = ScimUserLifecycleService(mappingRepository, userRepository, permissionRepository, groupMemberRepository)
      mutationService = ScimMutationService(organizationRepository, configurationRepository, transactions)
    }

    @AfterAll
    @JvmStatic
    fun tearDownDatabase() {
      context.close()
      container.close()
    }
  }
}
