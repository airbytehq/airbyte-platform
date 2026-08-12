/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.UserAuthIdRequestBody
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceReadList
import io.airbyte.commons.auth.config.InitialUserConfig
import io.airbyte.commons.auth.resolvers.GenericOidcUserAuthenticationResolver
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL_VERIFIED
import io.airbyte.commons.auth.support.UserAuthenticationResolver
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.ResourceBootstrapHandler
import io.airbyte.commons.server.handlers.ResourceBootstrapHandlerInterface
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.commons.server.support.SecurityAwareCurrentUserService
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.User
import io.airbyte.config.persistence.PermissionPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.auth.TokenType
import io.airbyte.data.repositories.ApplicationRepository
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.GroupMemberWithUserInfoRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.repositories.GroupWithMemberCountRepository
import io.airbyte.data.repositories.OrganizationDomainVerificationRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimAuthUserRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.UserInvitationRepository
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.repositories.entities.GroupMember
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.OrganizationDomainVerification
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.ScimAirbyteUser
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.ExternalUserService
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.ScimAuthUserOwnershipService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.ApplicationServiceDataImpl
import io.airbyte.data.services.impls.data.GroupServiceDataImpl
import io.airbyte.data.services.impls.data.PermissionServiceDataImpl
import io.airbyte.data.services.impls.data.UserInvitationServiceDataImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationMethod
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
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
import io.airbyte.domain.services.scim.ScimFirstLoginAttachmentResult
import io.airbyte.domain.services.scim.ScimFirstLoginService
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.domain.services.scim.ScimUserLifecycleService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig.OidcIdentityProviderConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig.OidcIdentityProviderConfig.GenericOidcFieldMappingConfig
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.http.HttpRequest
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.micronaut.security.token.jwt.validator.ReactiveJsonWebTokenValidator
import io.micronaut.security.utils.SecurityService
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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
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
  fun `verified current mapping email attaches one User across active and inactive organizations without side effects`() {
    val usersBefore = jooq.fetchCount(Tables.USER)
    val tenantA = tenant("first-login-a")
    val tenantB = tenant("first-login-b")
    val oldEmail = "pre-provisioned-old@example.com"
    val currentEmail = "pre-provisioned-current@example.com"
    val createdA =
      mutationService.execute(tenantA.context) {
        lifecycleService.create(tenantA.configurationId, tenantA.organizationId, input(true, userName = oldEmail))
      }
    val createdB =
      mutationService.execute(tenantB.context) {
        lifecycleService.create(tenantB.configurationId, tenantB.organizationId, input(false, userName = oldEmail))
      }
    mutationService.execute(tenantA.context) {
      lifecycleService.replace(
        tenantA.configurationId,
        tenantA.organizationId,
        createdA.id,
        input(true, userName = currentEmail),
      )
    }
    mutationService.execute(tenantB.context) {
      lifecycleService.replace(
        tenantB.configurationId,
        tenantB.organizationId,
        createdB.id,
        input(false, userName = currentEmail),
      )
    }
    val permissionsBefore = jooq.fetchCount(Tables.PERMISSION)
    val membershipsBefore = jooq.fetchCount(Tables.GROUP_MEMBER)
    val authUserId = "verified-first-login"

    val first =
      firstLoginService.attachIfPreProvisioned(
        currentEmail.uppercase(),
        currentEmail,
        authUserId,
        AuthProvider.KEYCLOAK,
      )
    val second =
      firstLoginService.attachIfPreProvisioned(
        currentEmail,
        currentEmail,
        authUserId,
        AuthProvider.KEYCLOAK,
      )

    assertThat(first).isEqualTo(ScimFirstLoginAttachmentResult.Attached(createdA.userId))
    assertThat(second).isEqualTo(ScimFirstLoginAttachmentResult.AlreadyAttached(createdA.userId))
    assertThat(createdB.userId).isEqualTo(createdA.userId)
    assertThat(jooq.fetchCount(Tables.USER)).isEqualTo(usersBefore + 1)
    assertThat(
      jooq
        .select(Tables.USER.EMAIL)
        .from(Tables.USER)
        .where(Tables.USER.ID.eq(createdA.userId))
        .fetchOne(Tables.USER.EMAIL),
    ).isEqualTo(oldEmail)
    assertThat(
      jooq
        .select(Tables.USER.STATUS)
        .from(Tables.USER)
        .where(Tables.USER.ID.eq(createdA.userId))
        .fetchOne(Tables.USER.STATUS),
    ).isNull()
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isEqualTo(1)
    assertThat(jooq.fetchCount(Tables.PERMISSION)).isEqualTo(permissionsBefore)
    assertThat(jooq.fetchCount(Tables.GROUP_MEMBER)).isEqualTo(membershipsBefore)
    assertThat(mappingRepository.findUser(createdA.id, tenantA.configurationId, tenantA.organizationId)?.userActive).isTrue()
    assertThat(mappingRepository.findUser(createdB.id, tenantB.configurationId, tenantB.organizationId)?.userActive).isFalse()
  }

  @Test
  fun `generic OIDC rejects a verified attachment email outside the source SSO organization domains`() {
    val sourceTenant = tenant("generic-oidc-domain-source", listOf("example.com", "claimed.example"))
    val mappedTenant = tenant("generic-oidc-domain-mapped", listOf("example.com", "restricted.example"))
    val configuredEmail = "generic-oidc@claimed.example"
    val verifiedEmail = "generic-oidc@restricted.example"
    val authUserId = "generic-oidc-domain-subject"
    val realm = "generic-oidc-domain-realm"
    val mapped =
      mutationService.execute(mappedTenant.context) {
        lifecycleService.create(
          mappedTenant.configurationId,
          mappedTenant.organizationId,
          input(true, verifiedEmail, "generic-oidc-domain", "Generic OIDC Domain User"),
        )
      }
    val securityService = mockk<SecurityService>()
    every { securityService.username() } returns Optional.of(authUserId)
    every { securityService.authentication } returns
      Optional.of(
        Authentication.build(
          authUserId,
          mapOf(
            "upn" to configuredEmail,
            "email" to verifiedEmail,
            JWT_USER_EMAIL_VERIFIED to true,
            "iss" to realm,
          ),
        ),
      )
    val authenticationResolver =
      GenericOidcUserAuthenticationResolver(
        securityService,
        AirbyteAuthConfig(
          identityProvider =
            AirbyteAuthIdentityProviderConfig(
              oidc =
                OidcIdentityProviderConfig(
                  fields = GenericOidcFieldMappingConfig(email = "upn"),
                ),
            ),
        ),
      )
    val organizationService = mockk<OrganizationService>()
    every { organizationService.getOrganizationBySsoConfigRealm(realm) } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(sourceTenant.organizationId)
          .withName("Generic OIDC Domain Source")
          .withEmail("source@claimed.example"),
      )
    val organizationEmailDomainService = mockk<OrganizationEmailDomainService>()
    every { organizationEmailDomainService.findByOrganizationId(sourceTenant.organizationId) } returns
      listOf(
        io.airbyte.config
          .OrganizationEmailDomain()
          .withOrganizationId(sourceTenant.organizationId)
          .withEmailDomain("claimed.example"),
      )
    every { organizationEmailDomainService.findByEmailDomain("claimed.example") } returns
      listOf(
        io.airbyte.config
          .OrganizationEmailDomain()
          .withOrganizationId(sourceTenant.organizationId)
          .withEmailDomain("claimed.example"),
      )
    every { organizationEmailDomainService.findByEmailDomain("restricted.example") } returns
      listOf(
        io.airbyte.config
          .OrganizationEmailDomain()
          .withOrganizationId(mappedTenant.organizationId)
          .withEmailDomain("restricted.example"),
      )
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(any(), any()) } returns false
    every {
      featureFlagClient.boolVariation(
        io.airbyte.featureflag.RestrictLoginsForSSODomains,
        any(),
      )
    } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        organizationService = organizationService,
        organizationEmailDomainService = organizationEmailDomainService,
        featureFlagClient = featureFlagClient,
      )

    val login =
      runCatching {
        handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
      }

    assertThat(login.exceptionOrNull())
      .isInstanceOf(io.airbyte.commons.server.errors.OperationNotAllowedException::class.java)
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isZero()
    assertThat(mappingRepository.findUser(mapped.id, mappedTenant.configurationId, mappedTenant.organizationId)).isNotNull()
  }

  @Test
  fun `new login commits User before production workspace and organization bootstrap`() {
    val dataplaneGroupId = UUID.randomUUID()
    val infrastructureOrganizationId = UUID.randomUUID()
    val authUserId = "committed-bootstrap-user"
    val email = "committed-bootstrap@example.com"
    organizationRepository.save(
      Organization(
        id = infrastructureOrganizationId,
        name = "Infrastructure Organization",
        email = "infrastructure@example.com",
      ),
    )
    context.getBean(DataplaneGroupRepository::class.java).save(
      DataplaneGroup(
        id = dataplaneGroupId,
        organizationId = infrastructureOrganizationId,
        name = "Default Test Dataplane Group",
        enabled = true,
        tombstone = false,
      ),
    )

    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("Committed Bootstrap User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns null
    val securityService = mockk<SecurityService>()
    every { securityService.username() } returns Optional.of(authUserId)
    val currentUserService = SecurityAwareCurrentUserService(userPersistence, securityService, transactions)
    val secretsRepositoryWriter = mockk<SecretsRepositoryWriter>(relaxed = true)
    every {
      secretsRepositoryWriter.createFromConfigLegacy(any(), any(), any(), any())
    } answers {
      secondArg()
    }
    val workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        mockk(relaxed = true),
        mockk<SecretsRepositoryReader>(relaxed = true),
        secretsRepositoryWriter,
        mockk<SecretPersistenceConfigService>(relaxed = true),
        mockk<MetricClient>(relaxed = true),
      )
    var workspaceWriteObservedCommittedUser = false
    val transactionCheckingWorkspaceService =
      object : WorkspaceService by workspaceService {
        override fun writeWorkspaceWithSecrets(workspace: io.airbyte.config.StandardWorkspace) {
          assertThat(transactions.hasConnection()).isFalse()
          assertThat(userPersistence.getUserByAuthId(authUserId)).isPresent
          workspaceWriteObservedCommittedUser = true
          workspaceService.writeWorkspaceWithSecrets(workspace)
        }
      }
    val uuidSupplier = Supplier { UUID.randomUUID() }
    val productionPermissionService =
      PermissionServiceDataImpl(
        transactionCheckingWorkspaceService,
        permissionRepository,
        configurationRepository,
        mappingRepository,
      )
    val permissionHandler =
      PermissionHandler(
        permissionPersistence,
        transactionCheckingWorkspaceService,
        uuidSupplier,
        productionPermissionService,
      )
    val organizationService = context.getBean(OrganizationService::class.java)
    val roleResolver =
      RoleResolver(
        context.getBean(AuthenticationHeaderResolver::class.java),
        currentUserService,
        null,
        permissionHandler,
      )
    val resourceBootstrapHandler =
      ResourceBootstrapHandler(
        uuidSupplier,
        transactionCheckingWorkspaceService,
        organizationService,
        permissionHandler,
        currentUserService,
        roleResolver,
        mockk<OrganizationPaymentConfigService>(relaxed = true),
        AirbyteEdition.COMMUNITY,
        mockk<DataplaneGroupService> {
          every { getDefaultDataplaneGroup() } returns
            io.airbyte.config
              .DataplaneGroup()
              .withId(dataplaneGroupId)
              .withOrganizationId(infrastructureOrganizationId)
              .withName("Default Test Dataplane Group")
              .withEnabled(true)
              .withTombstone(false)
        },
        mockk<EntitlementService>(relaxed = true),
      )

    assertThat(organizationRepository.findByEmailIgnoreCase(email)).isEmpty()

    val response =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        organizationService = organizationService,
        permissionHandler = permissionHandler,
        resourceBootstrapHandler = resourceBootstrapHandler,
        uuidSupplier = uuidSupplier,
      ).getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))

    assertThat(response.newUserCreated).isTrue()
    assertThat(workspaceWriteObservedCommittedUser).isTrue()
    val createdOrganization = organizationRepository.findByEmailIgnoreCase(email).single()
    assertThat(createdOrganization.userId).isEqualTo(response.userRead.userId)
    assertThat(jooq.fetchCount(Tables.WORKSPACE, Tables.WORKSPACE.ORGANIZATION_ID.eq(createdOrganization.id))).isEqualTo(1)
    assertThat(response.userRead.defaultWorkspaceId).isNotNull()
  }

  @Test
  fun `SSO migration persists incoming identity before production workspace bootstrap`() {
    val tenant = tenant("sso-migration-bootstrap")
    val dataplaneGroupId = UUID.randomUUID()
    val email = "sso-migration-bootstrap@example.com"
    val oldAuthUserId = "sso-migration-bootstrap-old"
    val incomingAuthUserId = "sso-migration-bootstrap-incoming"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    context.getBean(DataplaneGroupRepository::class.java).save(
      DataplaneGroup(
        id = dataplaneGroupId,
        organizationId = tenant.organizationId,
        name = "SSO Migration Bootstrap Dataplane Group",
        enabled = true,
        tombstone = false,
      ),
    )

    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("SSO Migration Bootstrap")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "sso-migration-bootstrap-realm"
    val securityService = mockk<SecurityService>()
    every { securityService.username() } returns Optional.of(incomingAuthUserId)
    val currentUserService = SecurityAwareCurrentUserService(userPersistence, securityService, transactions)
    val secretsRepositoryWriter = mockk<SecretsRepositoryWriter>(relaxed = true)
    every {
      secretsRepositoryWriter.createFromConfigLegacy(any(), any(), any(), any())
    } answers {
      secondArg()
    }
    val workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        mockk(relaxed = true),
        mockk<SecretsRepositoryReader>(relaxed = true),
        secretsRepositoryWriter,
        mockk<SecretPersistenceConfigService>(relaxed = true),
        mockk<MetricClient>(relaxed = true),
      )
    var workspaceWriteObservedIncomingIdentity = false
    val identityCheckingWorkspaceService =
      object : WorkspaceService by workspaceService {
        override fun writeWorkspaceWithSecrets(workspace: io.airbyte.config.StandardWorkspace) {
          assertThat(transactions.hasConnection()).isTrue()
          assertThat(currentUserService.getCurrentUser().authUserId).isEqualTo(incomingAuthUserId)
          workspaceWriteObservedIncomingIdentity = true
          workspaceService.writeWorkspaceWithSecrets(workspace)
        }

        override fun writeWorkspaceWithSecrets(
          ctx: DSLContext,
          workspace: io.airbyte.config.StandardWorkspace,
        ) {
          assertThat(transactions.hasConnection()).isTrue()
          assertThat(currentUserService.getCurrentUser().authUserId).isEqualTo(incomingAuthUserId)
          workspaceWriteObservedIncomingIdentity = true
          workspaceService.writeWorkspaceWithSecrets(ctx, workspace)
        }
      }
    val uuidSupplier = Supplier { UUID.randomUUID() }
    val productionPermissionService =
      PermissionServiceDataImpl(
        identityCheckingWorkspaceService,
        permissionRepository,
        configurationRepository,
        mappingRepository,
      )
    val permissionHandler =
      PermissionHandler(
        permissionPersistence,
        identityCheckingWorkspaceService,
        uuidSupplier,
        productionPermissionService,
      )
    val organization =
      io.airbyte.config
        .Organization()
        .withOrganizationId(tenant.organizationId)
        .withName("SSO Migration Bootstrap")
        .withEmail("sso-migration-bootstrap-org@example.com")
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("sso-migration-bootstrap-realm") } returns Optional.of(organization)
    every { organizationService.getOrganization(tenant.organizationId) } returns Optional.of(organization)
    val roleResolver =
      RoleResolver(
        context.getBean(AuthenticationHeaderResolver::class.java),
        currentUserService,
        null,
        permissionHandler,
      )
    val resourceBootstrapHandler =
      ResourceBootstrapHandler(
        uuidSupplier,
        identityCheckingWorkspaceService,
        organizationService,
        permissionHandler,
        currentUserService,
        roleResolver,
        mockk<OrganizationPaymentConfigService>(relaxed = true),
        AirbyteEdition.COMMUNITY,
        mockk<DataplaneGroupService> {
          every { getDefaultDataplaneGroup() } returns
            io.airbyte.config
              .DataplaneGroup()
              .withId(dataplaneGroupId)
              .withOrganizationId(tenant.organizationId)
              .withName("SSO Migration Bootstrap Dataplane Group")
              .withEnabled(true)
              .withTombstone(false)
        },
        mockk<EntitlementService>(relaxed = true),
      )
    val workspacesHandler = mockk<WorkspacesHandler>()
    every { workspacesHandler.listWorkspacesInOrganization(any()) } returns WorkspaceReadList().workspaces(emptyList())
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(oldAuthUserId) } returns "sso-migration-bootstrap-legacy-realm"
    val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
    every { ssoConfigService.getSsoConfigByRealmName("sso-migration-bootstrap-legacy-realm") } returns null
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(any(), any()) } returns true

    val response =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        ssoConfigService = ssoConfigService,
        permissionHandler = permissionHandler,
        workspacesHandler = workspacesHandler,
        resourceBootstrapHandler = resourceBootstrapHandler,
        featureFlagClient = featureFlagClient,
        uuidSupplier = uuidSupplier,
      ).getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))

    assertThat(response.userRead.userId).isEqualTo(existingUser.userId)
    assertThat(response.newUserCreated).isFalse()
    assertThat(workspaceWriteObservedIncomingIdentity).isTrue()
    assertThat(userPersistence.getUserByAuthId(incomingAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
    assertThat(userPersistence.getUserByAuthId(oldAuthUserId)).isEmpty()
    assertThat(jooq.fetchCount(Tables.WORKSPACE, Tables.WORKSPACE.ORGANIZATION_ID.eq(tenant.organizationId))).isEqualTo(1)
    assertThat(response.userRead.defaultWorkspaceId).isNotNull()
  }

  @Test
  fun `SSO login and SCIM POST acquire configuration before email without deadlock`() {
    val tenant = tenant("sso-login-lock-order")
    val email = "sso-login-lock-order@example.com"
    val authUserId = "sso-login-lock-order-subject"
    val loginUserId = UUID.randomUUID()
    val loginReachedDecision = CountDownLatch(1)
    val releaseLogin = CountDownLatch(1)
    val concurrentFirstLoginService =
      spyk(
        ScimFirstLoginService(
          mappingRepository,
          userRepository,
          context.getBean(ScimAuthUserRepository::class.java),
        ),
      )
    every {
      concurrentFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        tenant.organizationId,
      )
    } answers {
      callOriginal().also {
        check(it == ScimFirstLoginAttachmentResult.NoMatch)
        loginReachedDecision.countDown()
        check(releaseLogin.await(10, TimeUnit.SECONDS))
      }
    }
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("SSO Login User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "sso-login-lock-order-realm"
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("sso-login-lock-order-realm") } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(tenant.organizationId)
          .withName("SSO Login Lock Order")
          .withEmail(email),
      )
    val permissionHandler =
      PermissionHandler(
        permissionPersistence,
        mockk<WorkspaceService>(relaxed = true),
        Supplier { UUID.randomUUID() },
        permissionService,
      )
    val workspacesHandler = mockk<WorkspacesHandler>()
    every { workspacesHandler.listWorkspacesInOrganization(any()) } returns
      WorkspaceReadList().workspaces(listOf(WorkspaceRead().workspaceId(UUID.randomUUID())))
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = loginUserId,
        organizationService = organizationService,
        permissionHandler = permissionHandler,
        workspacesHandler = workspacesHandler,
        featureFlagClient = featureFlagClient,
        attachmentService = concurrentFirstLoginService,
      )
    val initialWaiters = lockWaiterCount()
    val executor = Executors.newFixedThreadPool(2)

    try {
      val loginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
          }
        }
      check(loginReachedDecision.await(10, TimeUnit.SECONDS))

      val scimFuture =
        executor.submit<Result<io.airbyte.domain.models.scim.ScimUserRead>> {
          runCatching {
            mutationService.execute(tenant.context) {
              lifecycleService.create(
                tenant.configurationId,
                tenant.organizationId,
                input(true, email, "sso-login-lock-order", "SSO Login User"),
              )
            }
          }
        }
      check(waitForLockWaiters(initialWaiters + 1))
      releaseLogin.countDown()

      val login = loginFuture.get(30, TimeUnit.SECONDS).getOrThrow()
      val mapping = scimFuture.get(30, TimeUnit.SECONDS).getOrThrow()
      assertThat(login.newUserCreated).isTrue()
      assertThat(mapping.userId).isEqualTo(login.userRead.userId)
      assertThat(userPersistence.getUserByAuthId(authUserId).orElseThrow().userId).isEqualTo(loginUserId)
    } finally {
      releaseLogin.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `DSR deletion winning after SSO realm resolution prevents cross-organization attachment`() {
    val sourceTenant = tenant("dsr-login-source")
    val mappedTenant = tenant("dsr-login-mapped")
    val email = "dsr-login-race@example.com"
    val authUserId = "dsr-login-race-subject"
    val mapped =
      mutationService.execute(mappedTenant.context) {
        lifecycleService.create(
          mappedTenant.configurationId,
          mappedTenant.organizationId,
          input(true, email, "dsr-login-race", "DSR Login Race"),
        )
      }
    val attachmentStarted = CountDownLatch(1)
    val allowAttachment = CountDownLatch(1)
    val concurrentFirstLoginService =
      spyk(
        ScimFirstLoginService(
          mappingRepository,
          userRepository,
          context.getBean(ScimAuthUserRepository::class.java),
        ),
      )
    every {
      concurrentFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        sourceTenant.organizationId,
      )
    } answers {
      attachmentStarted.countDown()
      check(allowAttachment.await(10, TimeUnit.SECONDS))
      callOriginal()
    }
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("DSR Login Race")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "dsr-login-race-realm"
    val organizationService = mockk<OrganizationService>()
    every { organizationService.getOrganizationBySsoConfigRealm("dsr-login-race-realm") } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(sourceTenant.organizationId)
          .withName("DSR Login Source")
          .withEmail("dsr-login-source@example.com"),
      )
    val featureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
    every {
      featureFlagClient.boolVariation(
        io.airbyte.featureflag.BypassSsoDomainValidationEnforcement,
        any(),
      )
    } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        organizationService = organizationService,
        featureFlagClient = featureFlagClient,
        attachmentService = concurrentFirstLoginService,
      )
    val executor = Executors.newSingleThreadExecutor()

    try {
      val loginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
          }
        }
      check(attachmentStarted.await(10, TimeUnit.SECONDS))

      database.transaction { ctx ->
        ctx
          .deleteFrom(Tables.SCIM_CONFIGURATION)
          .where(Tables.SCIM_CONFIGURATION.ID.eq(sourceTenant.configurationId))
          .execute()
        ctx
          .deleteFrom(Tables.ORGANIZATION)
          .where(Tables.ORGANIZATION.ID.eq(sourceTenant.organizationId))
          .execute()
      }
      allowAttachment.countDown()

      val login = loginFuture.get(30, TimeUnit.SECONDS)
      assertThat(login.exceptionOrNull())
        .isInstanceOf(io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem::class.java)
      assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isZero()
      assertThat(mappingRepository.findUser(mapped.id, mappedTenant.configurationId, mappedTenant.organizationId)).isNotNull()
    } finally {
      allowAttachment.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `multi-owner raw SSO subject fails before application or external identity deletion`() {
    val tenant = tenant("multi-owner-sso-subject")
    val email = "multi-owner-sso-subject@example.com"
    val incomingAuthUserId = "multi-owner-sso-subject"
    val existingAuthUserId = "multi-owner-sso-existing-subject"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, existingAuthUserId, AuthProvider.KEYCLOAK)
    val firstOwner = ordinaryUser("multi-owner-sso-first@example.com")
    val secondOwner = ordinaryUser("multi-owner-sso-second@example.com")
    userPersistence.writeUser(firstOwner)
    userPersistence.writeUser(secondOwner)
    insertAuthUser(firstOwner.userId, incomingAuthUserId, AuthProvider.KEYCLOAK)
    insertAuthUser(secondOwner.userId, incomingAuthUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM)

    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("Multi-owner SSO Subject")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "multi-owner-new-sso-realm"
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("multi-owner-new-sso-realm") } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(tenant.organizationId),
      )
    val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
    every { ssoConfigService.getSsoConfigByRealmName("multi-owner-legacy-realm") } returns null
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns "multi-owner-legacy-realm"
    val applicationService = mockk<ApplicationService>(relaxed = true)
    every { applicationService.listApplicationsByUser(any()) } returns
      listOf(
        io.airbyte.config
          .Application()
          .withId("multi-owner-legacy-application"),
      )
    val featureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
    every {
      featureFlagClient.boolVariation(
        io.airbyte.featureflag.BypassSsoDomainValidationEnforcement,
        any(),
      )
    } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        ssoConfigService = ssoConfigService,
        applicationService = Optional.of(applicationService),
        featureFlagClient = featureFlagClient,
      )

    assertThatThrownBy {
      handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
    }.isInstanceOf(io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem::class.java)

    verify(exactly = 0) { applicationService.deleteApplication(any(), any()) }
    verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) }
    verify(exactly = 0) { externalUserService.deleteUserByExternalId(any(), any()) }
    assertThat(userPersistence.getUserByAuthId(existingAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(incomingAuthUserId))).isEqualTo(2)
  }

  @Test
  fun `SSO migration preserves applications when an old raw subject has another owner`() {
    val tenant = tenant("old-subject-collision")
    val email = "old-subject-collision@example.com"
    val oldAuthUserId = "old-subject-collision"
    val incomingAuthUserId = "old-subject-collision-incoming"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    val application =
      applicationService.createApplication(
        io.airbyte.config.helpers.AuthenticatedUserConverter.toAuthenticatedUser(
          existingUser,
          oldAuthUserId,
          AuthProvider.KEYCLOAK,
        ),
        "Preserved Application",
      )
    val otherOwner = ordinaryUser("old-subject-other-owner@example.com")
    userPersistence.writeUser(otherOwner)
    insertAuthUser(otherOwner.userId, oldAuthUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM)

    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("Old Subject Collision")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "old-subject-sso-realm"
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("old-subject-sso-realm") } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(tenant.organizationId),
      )
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(oldAuthUserId) } returns "old-subject-legacy-realm"
    val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
    every { ssoConfigService.getSsoConfigByRealmName("old-subject-legacy-realm") } returns null
    val featureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
    every {
      featureFlagClient.boolVariation(
        io.airbyte.featureflag.BypassSsoDomainValidationEnforcement,
        any(),
      )
    } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        ssoConfigService = ssoConfigService,
        applicationService = Optional.of(applicationService),
        featureFlagClient = featureFlagClient,
      )

    assertThatThrownBy {
      handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
    }.isInstanceOf(IllegalStateException::class.java)

    assertThat(jooq.fetchCount(Tables.APPLICATION, Tables.APPLICATION.ID.eq(UUID.fromString(application.id)))).isEqualTo(1)
    verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) }
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(oldAuthUserId))).isEqualTo(2)
    assertThat(userPersistence.getUserByAuthId(incomingAuthUserId)).isEmpty()
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(incomingAuthUserId))).isZero()
  }

  @Test
  fun `SSO migration cleanup failure retains durable pending identities and retry completes migration`() {
    val tenant = tenant("sso-cleanup-retry")
    val email = "sso-cleanup-retry@example.com"
    val oldAuthUserId = "sso-cleanup-retry-old"
    val incomingAuthUserId = "sso-cleanup-retry-incoming"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    val existingApplication =
      applicationService.createApplication(
        io.airbyte.config.helpers.AuthenticatedUserConverter.toAuthenticatedUser(
          existingUser,
          oldAuthUserId,
          AuthProvider.KEYCLOAK,
        ),
        "SSO Cleanup Retry Application",
      )

    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("SSO Cleanup Retry")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "sso-cleanup-retry-realm"
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("sso-cleanup-retry-realm") } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(tenant.organizationId),
      )
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(oldAuthUserId) } returns "sso-cleanup-retry-legacy-realm"
    every { externalUserService.deleteUserByEmailOnOtherRealms(email, "sso-cleanup-retry-realm") } throws
      ExpectedFailure() andThen
      Unit
    val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
    every { ssoConfigService.getSsoConfigByRealmName("sso-cleanup-retry-legacy-realm") } returns null
    val permissionHandler = mockk<PermissionHandler>(relaxed = true)
    every { permissionHandler.listPermissionsForOrganization(tenant.organizationId) } returns
      listOf(
        io.airbyte.config
          .UserPermission()
          .withUser(existingUser),
      )
    val workspacesHandler = mockk<WorkspacesHandler>()
    every {
      workspacesHandler.listWorkspacesInOrganization(
        io.airbyte.api.model.generated
          .ListWorkspacesInOrganizationRequestBody()
          .organizationId(tenant.organizationId),
      )
    } returns WorkspaceReadList().workspaces(listOf(WorkspaceRead().workspaceId(UUID.randomUUID())))
    val featureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
    every {
      featureFlagClient.boolVariation(
        io.airbyte.featureflag.BypassSsoDomainValidationEnforcement,
        any(),
      )
    } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        ssoConfigService = ssoConfigService,
        applicationService = Optional.of(applicationService),
        permissionHandler = permissionHandler,
        workspacesHandler = workspacesHandler,
        featureFlagClient = featureFlagClient,
      )

    assertThatThrownBy {
      handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
    }.isInstanceOf(ExpectedFailure::class.java)

    assertThat(userPersistence.getUserByAuthId(incomingAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
    assertThat(userPersistence.getUserByAuthId(oldAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
    assertThat(jooq.fetchCount(Tables.APPLICATION, Tables.APPLICATION.ID.eq(UUID.fromString(existingApplication.id)))).isZero()

    val result = handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))

    assertThat(result.userRead.userId).isEqualTo(existingUser.userId)
    assertThat(userPersistence.getUserByAuthId(incomingAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
    assertThat(userPersistence.getUserByAuthId(oldAuthUserId)).isEmpty()
    verify(exactly = 2) { externalUserService.deleteUserByEmailOnOtherRealms(email, "sso-cleanup-retry-realm") }
  }

  @Test
  fun `concurrent SSO cleanup retries finalize authentication once`() {
    val tenant = tenant("sso-cleanup-concurrent-retry")
    val email = "sso-cleanup-concurrent-retry@example.com"
    val oldAuthUserId = "sso-cleanup-concurrent-retry-old"
    val incomingAuthUserId = "sso-cleanup-concurrent-retry-incoming"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    userPersistence.writeAuthUser(existingUser.userId, incomingAuthUserId, AuthProvider.KEYCLOAK)

    val secondLoginResolved = CountDownLatch(1)
    val resolveCount = AtomicInteger()
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } answers {
      if (resolveCount.incrementAndGet() == 2) {
        secondLoginResolved.countDown()
      }
      AuthenticatedUser()
        .withEmail(email)
        .withName("Concurrent SSO Cleanup Retry")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    }
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "sso-cleanup-concurrent-retry-realm"
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("sso-cleanup-concurrent-retry-realm") } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(tenant.organizationId),
      )
    val cleanupStarted = CountDownLatch(1)
    val releaseCleanup = CountDownLatch(1)
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every {
      externalUserService.deleteUserByEmailOnOtherRealms(email, "sso-cleanup-concurrent-retry-realm")
    } answers {
      cleanupStarted.countDown()
      check(releaseCleanup.await(10, TimeUnit.SECONDS))
    }
    val featureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
    every {
      featureFlagClient.boolVariation(
        io.airbyte.featureflag.BypassSsoDomainValidationEnforcement,
        any(),
      )
    } returns true
    val firstHandler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        featureFlagClient = featureFlagClient,
      )
    val secondHandler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        featureFlagClient = featureFlagClient,
      )
    val executor = Executors.newFixedThreadPool(2)

    try {
      val firstLogin =
        executor.submit<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse> {
          firstHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
        }
      assertThat(cleanupStarted.await(10, TimeUnit.SECONDS)).isTrue()
      val secondLogin =
        executor.submit<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse> {
          secondHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
        }
      assertThat(secondLoginResolved.await(10, TimeUnit.SECONDS)).isTrue()
      assertThat(secondLogin.isDone).isFalse()

      releaseCleanup.countDown()

      assertThat(firstLogin.get(30, TimeUnit.SECONDS).userRead.userId).isEqualTo(existingUser.userId)
      assertThat(secondLogin.get(30, TimeUnit.SECONDS).userRead.userId).isEqualTo(existingUser.userId)
    } finally {
      releaseCleanup.countDown()
      executor.shutdownNow()
    }

    verify(exactly = 1) {
      externalUserService.deleteUserByEmailOnOtherRealms(email, "sso-cleanup-concurrent-retry-realm")
    }
    assertThat(userPersistence.getUserByAuthId(incomingAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
    assertThat(userPersistence.getUserByAuthId(oldAuthUserId)).isEmpty()
  }

  @Test
  fun `SSO migration bootstrap failure rolls back incoming identity`() {
    val tenant = tenant("sso-bootstrap-rollback")
    val email = "sso-bootstrap-rollback@example.com"
    val oldAuthUserId = "sso-bootstrap-rollback-old"
    val incomingAuthUserId = "sso-bootstrap-rollback-incoming"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    val existingApplication =
      applicationService.createApplication(
        io.airbyte.config.helpers.AuthenticatedUserConverter.toAuthenticatedUser(
          existingUser,
          oldAuthUserId,
          AuthProvider.KEYCLOAK,
        ),
        "SSO Bootstrap Rollback Application",
      )

    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("SSO Bootstrap Rollback")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "sso-bootstrap-rollback-realm"
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("sso-bootstrap-rollback-realm") } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(tenant.organizationId),
      )
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(oldAuthUserId) } returns "sso-bootstrap-rollback-legacy-realm"
    val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
    every { ssoConfigService.getSsoConfigByRealmName("sso-bootstrap-rollback-legacy-realm") } returns null
    val permissionHandler = mockk<PermissionHandler>(relaxed = true)
    every { permissionHandler.listPermissionsForOrganization(tenant.organizationId) } returns
      listOf(
        io.airbyte.config
          .UserPermission()
          .withUser(existingUser),
      )
    val workspacesHandler = mockk<WorkspacesHandler>()
    every { workspacesHandler.listWorkspacesInOrganization(any()) } returns WorkspaceReadList().workspaces(emptyList())
    val resourceBootstrapHandler = mockk<ResourceBootstrapHandlerInterface>()
    every { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any(), any()) } throws ExpectedFailure()
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        ssoConfigService = ssoConfigService,
        applicationService = Optional.of(applicationService),
        permissionHandler = permissionHandler,
        workspacesHandler = workspacesHandler,
        resourceBootstrapHandler = resourceBootstrapHandler,
        featureFlagClient = featureFlagClient,
      )

    assertThatThrownBy {
      handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
    }.isInstanceOf(ExpectedFailure::class.java)

    assertThat(userPersistence.getUserByAuthId(oldAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
    assertThat(userPersistence.getUserByAuthId(incomingAuthUserId)).isEmpty()
    assertThat(jooq.fetchCount(Tables.APPLICATION, Tables.APPLICATION.ID.eq(UUID.fromString(existingApplication.id)))).isEqualTo(1)
    verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(email, "sso-bootstrap-rollback-realm") }
  }

  @Test
  fun `inactive SCIM POST between SSO migration phases prevents identity cleanup`() {
    val tenant = tenant("sso-migration-inactive-gap")
    val email = "sso-migration-inactive-gap@example.com"
    val oldAuthUserId = "sso-migration-inactive-gap-old"
    val incomingAuthUserId = "sso-migration-inactive-gap-incoming"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    val existingApplication =
      applicationService.createApplication(
        io.airbyte.config.helpers.AuthenticatedUserConverter.toAuthenticatedUser(
          existingUser,
          oldAuthUserId,
          AuthProvider.KEYCLOAK,
        ),
        "SSO Migration Gap Application",
      )
    val cleanupApplicationService = spyk(applicationService)

    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("SSO Migration Inactive Gap")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "sso-migration-inactive-gap-realm"
    val organization =
      io.airbyte.config
        .Organization()
        .withOrganizationId(tenant.organizationId)
        .withName("SSO Migration Inactive Gap")
        .withEmail("sso-migration-inactive-gap-org@example.com")
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("sso-migration-inactive-gap-realm") } returns Optional.of(organization)
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(oldAuthUserId) } returns "sso-migration-inactive-gap-legacy-realm"
    val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
    every { ssoConfigService.getSsoConfigByRealmName("sso-migration-inactive-gap-legacy-realm") } returns null
    val permissionHandler =
      PermissionHandler(
        permissionPersistence,
        mockk<WorkspaceService>(relaxed = true),
        Supplier { UUID.randomUUID() },
        permissionService,
      )
    val workspacesHandler = mockk<WorkspacesHandler>()
    every { workspacesHandler.listWorkspacesInOrganization(any()) } returns
      WorkspaceReadList().workspaces(listOf(WorkspaceRead().workspaceId(UUID.randomUUID())))
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    val phaseOneCommitted = CountDownLatch(1)
    val releaseMigration = CountDownLatch(1)
    val transactionExecutions = AtomicInteger()
    val boundaryTransactions =
      object : TransactionOperations<Connection> {
        override fun getConnection(): Connection = transactions.connection

        override fun hasConnection(): Boolean = transactions.hasConnection()

        override fun findTransactionStatus(): Optional<out io.micronaut.transaction.TransactionStatus<*>> = transactions.findTransactionStatus()

        override fun <R> execute(
          definition: io.micronaut.transaction.TransactionDefinition,
          callback: io.micronaut.transaction.TransactionCallback<Connection, R>,
        ): R {
          val result = transactions.execute(definition, callback)
          if (transactionExecutions.incrementAndGet() == 1) {
            phaseOneCommitted.countDown()
            check(releaseMigration.await(10, TimeUnit.SECONDS))
          }
          return result
        }
      }
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        ssoConfigService = ssoConfigService,
        applicationService = Optional.of(cleanupApplicationService),
        permissionHandler = permissionHandler,
        workspacesHandler = workspacesHandler,
        featureFlagClient = featureFlagClient,
        transactionOperations = boundaryTransactions,
      )
    val executor = Executors.newSingleThreadExecutor()

    try {
      val loginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
          }
        }
      check(phaseOneCommitted.await(10, TimeUnit.SECONDS))

      val mapping =
        mutationService.execute(tenant.context) {
          lifecycleService.create(
            tenant.configurationId,
            tenant.organizationId,
            input(false, email, "sso-migration-inactive-gap", "SSO Migration Inactive Gap"),
          )
        }
      releaseMigration.countDown()
      val loginResult = loginFuture.get(30, TimeUnit.SECONDS)

      assertThat(loginResult.isFailure).isTrue()
      verify(exactly = 0) { cleanupApplicationService.deleteApplication(any(), any()) }
      verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) }
      assertThat(jooq.fetchCount(Tables.APPLICATION, Tables.APPLICATION.ID.eq(UUID.fromString(existingApplication.id)))).isEqualTo(1)
      assertThat(userPersistence.getUserByAuthId(oldAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
      assertThat(userPersistence.getUserByAuthId(incomingAuthUserId)).isEmpty()
      assertThat(mapping.userId).isEqualTo(existingUser.userId)
      assertThat(mapping.active).isFalse()
    } finally {
      releaseMigration.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `SCIM ownership moving during orphan realm resolution prevents stale global email relink`() {
    val tenant = tenant("orphan-relink-stale-global")
    val loginEmail = "orphan-relink-stale-global@example.com"
    val currentMappingEmail = "orphan-relink-current@example.com"
    val oldAuthUserId = "orphan-relink-stale-global-old"
    val incomingAuthUserId = "orphan-relink-stale-global-incoming"
    val existingUser = ordinaryUser(loginEmail)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    val existingApplication =
      applicationService.createApplication(
        io.airbyte.config.helpers.AuthenticatedUserConverter.toAuthenticatedUser(
          existingUser,
          oldAuthUserId,
          AuthProvider.KEYCLOAK,
        ),
        "Orphan Relink Stale Global Application",
      )
    val cleanupApplicationService = spyk(applicationService)
    val realmResolutionStarted = CountDownLatch(1)
    val releaseRealmResolution = CountDownLatch(1)
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(oldAuthUserId) } answers {
      assertThat(transactions.hasConnection()).isFalse()
      realmResolutionStarted.countDown()
      check(releaseRealmResolution.await(10, TimeUnit.SECONDS))
      null
    }
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(loginEmail)
        .withName("Orphan Relink Stale Global")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns loginEmail
    every { authenticationResolver.resolveRealm() } returns null
    val permissionHandler = mockk<PermissionHandler>(relaxed = true)
    val resourceBootstrapHandler = mockk<ResourceBootstrapHandlerInterface>(relaxed = true)
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        applicationService = Optional.of(cleanupApplicationService),
        permissionHandler = permissionHandler,
        resourceBootstrapHandler = resourceBootstrapHandler,
      )
    val executor = Executors.newSingleThreadExecutor()

    try {
      val loginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
          }
        }
      check(realmResolutionStarted.await(10, TimeUnit.SECONDS))

      val mapping =
        mutationService.execute(tenant.context) {
          lifecycleService.create(
            tenant.configurationId,
            tenant.organizationId,
            input(false, loginEmail, "orphan-relink-stale-global", "Orphan Relink Stale Global"),
          )
        }
      mutationService.execute(tenant.context) {
        lifecycleService.replace(
          tenant.configurationId,
          tenant.organizationId,
          mapping.id,
          input(false, currentMappingEmail, "orphan-relink-stale-global", "Orphan Relink Stale Global"),
        )
      }
      val authUsersAfterMappingTransition = jooq.fetchCount(Tables.AUTH_USER)
      val applicationsAfterMappingTransition = jooq.fetchCount(Tables.APPLICATION)
      val permissionsAfterMappingTransition = jooq.fetchCount(Tables.PERMISSION)
      releaseRealmResolution.countDown()
      val loginResult = loginFuture.get(30, TimeUnit.SECONDS)

      assertThat(loginResult.exceptionOrNull())
        .isInstanceOf(io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem::class.java)
      assertThat(mapping.userId).isEqualTo(existingUser.userId)
      assertThat(userPersistence.getUserByAuthId(oldAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
      assertThat(userPersistence.getUserByAuthId(incomingAuthUserId)).isEmpty()
      assertThat(jooq.fetchCount(Tables.AUTH_USER)).isEqualTo(authUsersAfterMappingTransition)
      assertThat(jooq.fetchCount(Tables.APPLICATION, Tables.APPLICATION.ID.eq(UUID.fromString(existingApplication.id)))).isEqualTo(1)
      assertThat(jooq.fetchCount(Tables.APPLICATION)).isEqualTo(applicationsAfterMappingTransition)
      assertThat(jooq.fetchCount(Tables.PERMISSION)).isEqualTo(permissionsAfterMappingTransition)
      verify(exactly = 0) { cleanupApplicationService.deleteApplication(any(), any()) }
      verify(exactly = 0) { permissionHandler.createPermission(any()) }
      verify(exactly = 0) { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any(), any()) }
      verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) }
    } finally {
      releaseRealmResolution.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `SCIM ownership moving during legacy realm resolution prevents stale global email SSO migration`() {
    val tenant = tenant("sso-migration-stale-global")
    val loginEmail = "sso-migration-stale-global@example.com"
    val currentMappingEmail = "sso-migration-current@example.com"
    val oldAuthUserId = "sso-migration-stale-global-old"
    val incomingAuthUserId = "sso-migration-stale-global-incoming"
    val existingUser = ordinaryUser(loginEmail)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    val existingApplication =
      applicationService.createApplication(
        io.airbyte.config.helpers.AuthenticatedUserConverter.toAuthenticatedUser(
          existingUser,
          oldAuthUserId,
          AuthProvider.KEYCLOAK,
        ),
        "SSO Migration Stale Global Application",
      )
    val cleanupApplicationService = spyk(applicationService)
    val realmResolutionStarted = CountDownLatch(1)
    val releaseRealmResolution = CountDownLatch(1)
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(oldAuthUserId) } answers {
      assertThat(transactions.hasConnection()).isFalse()
      realmResolutionStarted.countDown()
      check(releaseRealmResolution.await(10, TimeUnit.SECONDS))
      "sso-migration-stale-global-legacy-realm"
    }
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(loginEmail)
        .withName("SSO Migration Stale Global")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns loginEmail
    every { authenticationResolver.resolveRealm() } returns "sso-migration-stale-global-realm"
    val organization =
      io.airbyte.config
        .Organization()
        .withOrganizationId(tenant.organizationId)
        .withName("SSO Migration Stale Global")
        .withEmail("sso-migration-stale-global-org@example.com")
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("sso-migration-stale-global-realm") } returns Optional.of(organization)
    val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
    every { ssoConfigService.getSsoConfigByRealmName("sso-migration-stale-global-legacy-realm") } returns null
    val permissionHandler =
      PermissionHandler(
        permissionPersistence,
        mockk<WorkspaceService>(relaxed = true),
        Supplier { UUID.randomUUID() },
        permissionService,
      )
    val workspacesHandler = mockk<WorkspacesHandler>()
    every { workspacesHandler.listWorkspacesInOrganization(any()) } returns WorkspaceReadList().workspaces(emptyList())
    val defaultWorkspaceId = workspace(tenant.organizationId, "sso-migration-stale-global-workspace")
    val resourceBootstrapHandler = mockk<ResourceBootstrapHandlerInterface>()
    every { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any(), any()) } returns
      WorkspaceRead().workspaceId(defaultWorkspaceId)
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        ssoConfigService = ssoConfigService,
        applicationService = Optional.of(cleanupApplicationService),
        permissionHandler = permissionHandler,
        workspacesHandler = workspacesHandler,
        resourceBootstrapHandler = resourceBootstrapHandler,
        featureFlagClient = featureFlagClient,
      )
    val executor = Executors.newSingleThreadExecutor()

    try {
      val loginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
          }
        }
      check(realmResolutionStarted.await(10, TimeUnit.SECONDS))

      val mapping =
        mutationService.execute(tenant.context) {
          lifecycleService.create(
            tenant.configurationId,
            tenant.organizationId,
            input(false, loginEmail, "sso-migration-stale-global", "SSO Migration Stale Global"),
          )
        }
      mutationService.execute(tenant.context) {
        lifecycleService.replace(
          tenant.configurationId,
          tenant.organizationId,
          mapping.id,
          input(false, currentMappingEmail, "sso-migration-stale-global", "SSO Migration Stale Global"),
        )
      }
      val authUsersAfterMappingTransition = jooq.fetchCount(Tables.AUTH_USER)
      val applicationsAfterMappingTransition = jooq.fetchCount(Tables.APPLICATION)
      val permissionsAfterMappingTransition = jooq.fetchCount(Tables.PERMISSION)
      releaseRealmResolution.countDown()
      val loginResult = loginFuture.get(30, TimeUnit.SECONDS)

      assertThat(loginResult.exceptionOrNull())
        .isInstanceOf(io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem::class.java)
      assertThat(mapping.userId).isEqualTo(existingUser.userId)
      assertThat(userPersistence.getUserByAuthId(oldAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
      assertThat(userPersistence.getUserByAuthId(incomingAuthUserId)).isEmpty()
      assertThat(jooq.fetchCount(Tables.AUTH_USER)).isEqualTo(authUsersAfterMappingTransition)
      assertThat(jooq.fetchCount(Tables.APPLICATION, Tables.APPLICATION.ID.eq(UUID.fromString(existingApplication.id)))).isEqualTo(1)
      assertThat(jooq.fetchCount(Tables.APPLICATION)).isEqualTo(applicationsAfterMappingTransition)
      assertThat(jooq.fetchCount(Tables.PERMISSION)).isEqualTo(permissionsAfterMappingTransition)
      assertThat(userPersistence.getUser(existingUser.userId).orElseThrow().defaultWorkspaceId).isNull()
      verify(exactly = 0) { cleanupApplicationService.deleteApplication(any(), any()) }
      verify(exactly = 0) { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any(), any()) }
      verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) }
    } finally {
      releaseRealmResolution.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `restricted domain rejects an absent raw subject without deleting the external identity`() {
    val authUserId = "restricted-domain-absent-subject"
    val email = "restricted-domain-absent@example.com"
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("Restricted Domain User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "restricted-domain-realm"
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    val organizationEmailDomainService = mockk<OrganizationEmailDomainService>()
    every { organizationEmailDomainService.findByEmailDomain("example.com") } returns
      listOf(
        io.airbyte.config
          .OrganizationEmailDomain()
          .withOrganizationId(UUID.randomUUID())
          .withEmailDomain("example.com"),
      )
    val featureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
    every {
      featureFlagClient.boolVariation(
        io.airbyte.featureflag.RestrictLoginsForSSODomains,
        any(),
      )
    } returns true
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("restricted-domain-realm") } returns Optional.empty()

    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        organizationEmailDomainService = organizationEmailDomainService,
        featureFlagClient = featureFlagClient,
      )

    assertThatThrownBy {
      handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
    }.isInstanceOf(io.airbyte.api.problems.throwable.generated.SSORequiredProblem::class.java)

    verify(exactly = 0) { externalUserService.deleteUserByExternalId(any(), any()) }
    assertThat(jooq.fetchCount(Tables.USER, Tables.USER.EMAIL.equalIgnoreCase(email))).isZero()
  }

  @Test
  fun `no-match login conflict does not delete an unowned external subject`() {
    val email = "no-match-conflict@example.com"
    val existingAuthUserId = "no-match-existing-subject"
    val incomingAuthUserId = "no-match-incoming-subject"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, existingAuthUserId, AuthProvider.KEYCLOAK)
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("No Match Conflict")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns "no-match-incoming-realm"
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns "no-match-existing-realm"
    val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
    every { ssoConfigService.getSsoConfigByRealmName("no-match-existing-realm") } returns null
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("no-match-incoming-realm") } returns Optional.empty()

    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        ssoConfigService = ssoConfigService,
      )

    assertThatThrownBy {
      handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
    }.isInstanceOf(io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem::class.java)

    verify(exactly = 0) { externalUserService.deleteUserByExternalId(any(), any()) }
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(incomingAuthUserId))).isZero()
    assertThat(userPersistence.getUserByAuthId(existingAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
  }

  @Test
  fun `old global email cannot attach active or inactive SCIM Users with or without verification`() {
    listOf(true, false).forEach { active ->
      listOf(true, false).forEach { verified ->
        val suffix = "${if (active) "active" else "inactive"}-${if (verified) "verified" else "unverified"}"
        val tenant = tenant("old-email-$suffix")
        val oldEmail = "old-$suffix@example.com"
        val currentEmail = "current-$suffix@example.com"
        val mapped =
          mutationService.execute(tenant.context) {
            lifecycleService.create(
              tenant.configurationId,
              tenant.organizationId,
              input(active, oldEmail, "old-email-$suffix", "Mapped User"),
            )
          }
        mutationService.execute(tenant.context) {
          lifecycleService.replace(
            tenant.configurationId,
            tenant.organizationId,
            mapped.id,
            input(active, currentEmail, "old-email-$suffix", "Mapped User"),
          )
        }
        val authUserId = "old-email-$suffix-subject"
        val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
        every { authenticationResolver.resolveUser(authUserId) } returns
          AuthenticatedUser()
            .withEmail(oldEmail)
            .withName("Mapped User")
            .withAuthUserId(authUserId)
            .withAuthProvider(AuthProvider.KEYCLOAK)
        every { authenticationResolver.resolveVerifiedEmail() } returns oldEmail.takeIf { verified }
        every { authenticationResolver.resolveRealm() } returns null
        val handler =
          loginHandler(
            authenticationResolver = authenticationResolver,
            userId = UUID.randomUUID(),
          )

        assertThatThrownBy {
          handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
        }.isInstanceOf(io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem::class.java)

        assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isZero()
        assertThat(jooq.fetchCount(Tables.USER, Tables.USER.ID.eq(mapped.userId))).isEqualTo(1)
        assertThat(
          mappingRepository
            .findUser(mapped.id, tenant.configurationId, tenant.organizationId)
            ?.primaryEmail,
        ).isEqualTo(currentEmail)
      }
    }
  }

  @Test
  fun `raw authentication subject with different User owners fails closed for user and permission reads`() {
    val tenantA = tenant("raw-subject-owner-a")
    val tenantB = tenant("raw-subject-owner-b")
    val userA = userRepository.save(ScimAirbyteUser(name = "Owner A", email = "owner-a@example.com"))
    val userB = userRepository.save(ScimAirbyteUser(name = "Owner B", email = "owner-b@example.com"))
    permissionRepository.save(
      Permission(
        userId = userA.id,
        organizationId = tenantA.organizationId,
        permissionType = PermissionType.organization_admin,
      ),
    )
    permissionRepository.save(
      Permission(
        userId = userB.id,
        organizationId = tenantB.organizationId,
        permissionType = PermissionType.organization_admin,
      ),
    )
    val authUserId = "schema-valid-cross-provider-collision"
    insertAuthUser(userA.id!!, authUserId, AuthProvider.KEYCLOAK)
    insertAuthUser(userB.id!!, authUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM)

    assertThat(userPersistence.getUserByAuthId(authUserId)).isEmpty()
    assertThat(permissionService.getPermissionsByAuthUserId(authUserId)).isEmpty()
    assertThat(
      permissionPersistence.findPermissionTypeForUserAndOrganization(tenantA.organizationId, authUserId),
    ).isNull()
    assertThat(
      permissionPersistence.findPermissionTypeForUserAndOrganization(tenantB.organizationId, authUserId),
    ).isNull()

    val roleResolver = roleResolver()
    assertThat(
      roleResolver
        .newRequest()
        .withSubject(authUserId, TokenType.USER)
        .withOrg(tenantA.organizationId)
        .roles(),
    ).containsExactly(AuthRoleConstants.AUTHENTICATED_USER)
    assertThat(
      roleResolver
        .newRequest()
        .withSubject(authUserId, TokenType.USER)
        .withOrg(tenantB.organizationId)
        .roles(),
    ).containsExactly(AuthRoleConstants.AUTHENTICATED_USER)
  }

  @Test
  fun `same User cross-provider authentication rows do not duplicate permission reads`() {
    val tenant = tenant("same-owner-cross-provider")
    val user = userRepository.save(ScimAirbyteUser(name = "Same Owner", email = "same-owner@example.com"))
    val permission =
      permissionRepository.save(
        Permission(
          userId = user.id,
          organizationId = tenant.organizationId,
          permissionType = PermissionType.organization_admin,
        ),
      )
    val authUserId = "schema-valid-same-owner-cross-provider"
    insertAuthUser(user.id!!, authUserId, AuthProvider.KEYCLOAK)
    insertAuthUser(user.id!!, authUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM)

    assertThat(userPersistence.getUserByAuthId(authUserId).orElseThrow().userId).isEqualTo(user.id)
    assertThat(permissionService.getPermissionsByAuthUserId(authUserId).map { it.permissionId })
      .containsExactly(permission.id)
    assertThat(
      permissionPersistence.findPermissionTypeForUserAndOrganization(tenant.organizationId, authUserId),
    ).isEqualTo(io.airbyte.config.Permission.PermissionType.ORGANIZATION_ADMIN)
  }

  @Test
  fun `cross-provider first-login repeat preserves one identity and permission resolution`() {
    val tenant = tenant("cross-provider-first-login")
    val email = "cross-provider-first-login@example.com"
    val mapped =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true, userName = email))
      }
    val workspaceId = workspace(tenant.organizationId, "cross-provider-first-login-workspace")
    permissionRepository.save(
      Permission(
        userId = mapped.userId,
        workspaceId = workspaceId,
        permissionType = PermissionType.workspace_admin,
      ),
    )
    val authUserId = "cross-provider-first-login-subject"
    userPersistence.writeAuthUser(mapped.userId, authUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM)

    val unverifiedRepeat =
      firstLoginService.attachIfPreProvisioned(
        email,
        null,
        authUserId,
        AuthProvider.KEYCLOAK,
      )
    val verifiedRepeat =
      firstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
      )

    assertThat(unverifiedRepeat).isEqualTo(ScimFirstLoginAttachmentResult.AlreadyAttached(mapped.userId))
    assertThat(verifiedRepeat).isEqualTo(ScimFirstLoginAttachmentResult.AlreadyAttached(mapped.userId))
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isEqualTo(1)
    assertThat(
      permissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId, authUserId),
    ).isEqualTo(io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN)
    assertThat(
      permissionPersistence.findPermissionTypeForUserAndOrganization(tenant.organizationId, authUserId),
    ).isEqualTo(io.airbyte.config.Permission.PermissionType.ORGANIZATION_MEMBER)
  }

  @Test
  fun `unverified and absent verification claims never attach a matching pre-provisioned User`() {
    val tenant = tenant("first-login-unverified")
    val email = "unverified-mapped@example.com"
    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true, userName = email))
      }

    val unverified =
      firstLoginService.attachIfPreProvisioned(
        email,
        null,
        "unverified-auth-user",
        AuthProvider.KEYCLOAK,
      )
    val absent =
      firstLoginService.attachIfPreProvisioned(
        email,
        null,
        "absent-verification-auth-user",
        AuthProvider.KEYCLOAK,
      )

    assertThat(unverified).isEqualTo(ScimFirstLoginAttachmentResult.EmailNotVerified)
    assertThat(absent).isEqualTo(ScimFirstLoginAttachmentResult.EmailNotVerified)
    assertThat(jooq.fetchCount(Tables.AUTH_USER)).isZero()
    assertThat(jooq.fetchCount(Tables.USER)).isEqualTo(1)
    assertThat(userRepository.findById(created.userId)).isPresent
  }

  @Test
  fun `matching mappings for different Users fail closed with no identity write`() {
    val tenantA = tenant("first-login-conflict-a")
    val tenantB = tenant("first-login-conflict-b")
    val email = "mapping-conflict@example.com"
    val createdA =
      mutationService.execute(tenantA.context) {
        lifecycleService.create(tenantA.configurationId, tenantA.organizationId, input(true, userName = email))
      }
    val otherUser = userRepository.save(ScimAirbyteUser(name = "Other User", email = "other-user@example.com"))
    mappingRepository.save(
      ScimResourceMapping(
        scimConfigurationId = tenantB.configurationId,
        organizationId = tenantB.organizationId,
        resourceType = ScimResourceType.USER,
        userId = otherUser.id,
        externalId = "other-user",
        userName = email,
        primaryEmail = email,
        userActive = false,
        attributes = objectMapper.createObjectNode(),
      ),
    )

    val result =
      firstLoginService.attachIfPreProvisioned(
        email.uppercase(),
        email,
        "conflicting-mapping-auth-user",
        AuthProvider.KEYCLOAK,
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Conflict)
    assertThat(jooq.fetchCount(Tables.AUTH_USER)).isZero()
    assertThat(jooq.fetchCount(Tables.USER)).isEqualTo(2)
    assertThat(userRepository.findById(createdA.userId)).isPresent
    assertThat(userRepository.findById(otherUser.id)).isPresent
  }

  @Test
  fun `identity already attached to another User fails closed without replacing it`() {
    val tenant = tenant("first-login-identity-conflict")
    val email = "identity-conflict@example.com"
    val mapped =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true, userName = email))
      }
    val otherUser = userRepository.save(ScimAirbyteUser(name = "Other User", email = "identity-owner@example.com"))
    val authUserId = "identity-owned-elsewhere"
    userPersistence.writeAuthUser(otherUser.id, authUserId, AuthProvider.KEYCLOAK)

    val result =
      firstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Conflict)
    assertThat(jooq.fetchCount(Tables.AUTH_USER)).isEqualTo(1)
    assertThat(
      jooq
        .select(Tables.AUTH_USER.USER_ID)
        .from(Tables.AUTH_USER)
        .where(Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))
        .fetchOne(Tables.AUTH_USER.USER_ID),
    ).isEqualTo(otherUser.id)
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.USER_ID.eq(mapped.userId))).isZero()
  }

  @Test
  fun `same authentication id cannot attach different Users across providers or combine their roles`() {
    val tenant = tenant("first-login-shared-provider-mapped")
    val ownerTenant = tenant("first-login-shared-provider-owner")
    val email = "provider-aware-identity@example.com"
    val mapped =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true, userName = email))
      }
    val otherUser = userRepository.save(ScimAirbyteUser(name = "Other User", email = "other-provider@example.com"))
    permissionRepository.save(
      Permission(
        userId = otherUser.id,
        organizationId = ownerTenant.organizationId,
        permissionType = PermissionType.organization_admin,
      ),
    )
    val sharedAuthUserId = "shared-provider-auth-user-id"
    val mappedAuthUserId = "mapped-provider-auth-user-id"
    userPersistence.writeAuthUser(otherUser.id, sharedAuthUserId, AuthProvider.KEYCLOAK)
    userPersistence.writeAuthUser(mapped.userId, mappedAuthUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM)

    val conflict =
      firstLoginService.attachIfPreProvisioned(
        email,
        email,
        sharedAuthUserId,
        AuthProvider.GOOGLE_IDENTITY_PLATFORM,
      )

    assertThat(conflict).isEqualTo(ScimFirstLoginAttachmentResult.Conflict)
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(sharedAuthUserId))).isEqualTo(1)
    assertThat(userPersistence.getUserByAuthId(sharedAuthUserId).orElseThrow().userId).isEqualTo(otherUser.id)
    assertThat(userPersistence.getUserByAuthId(mappedAuthUserId).orElseThrow().userId).isEqualTo(mapped.userId)

    val permissionHandler = PermissionHandler(null, mockk<WorkspaceService>(), null, permissionService)
    val roleResolver =
      RoleResolver(
        context.getBean(AuthenticationHeaderResolver::class.java),
        mockk<CurrentUserService>(),
        null,
        permissionHandler,
      )
    assertThat(
      roleResolver
        .newRequest()
        .withSubject(sharedAuthUserId, TokenType.USER)
        .withOrg(tenant.organizationId)
        .roles(),
    ).containsExactly(AuthRoleConstants.AUTHENTICATED_USER)
    assertThat(
      roleResolver
        .newRequest()
        .withSubject(sharedAuthUserId, TokenType.USER)
        .withOrg(ownerTenant.organizationId)
        .roles(),
    ).contains(AuthRoleConstants.ORGANIZATION_ADMIN)
    assertThat(
      roleResolver
        .newRequest()
        .withSubject(mappedAuthUserId, TokenType.USER)
        .withOrg(tenant.organizationId)
        .roles(),
    ).contains(AuthRoleConstants.ORGANIZATION_MEMBER)
    assertThat(
      roleResolver
        .newRequest()
        .withSubject(mappedAuthUserId, TokenType.USER)
        .withOrg(ownerTenant.organizationId)
        .roles(),
    ).containsExactly(AuthRoleConstants.AUTHENTICATED_USER)
  }

  @Test
  fun `repeating the same normal authentication identity write is idempotent`() {
    val user = userRepository.save(ScimAirbyteUser(name = "Identity Owner", email = "identity-owner@example.com"))
    val authUserId = "idempotent-normal-auth-user"

    assertThat(userPersistence.writeAuthUser(user.id, authUserId, AuthProvider.KEYCLOAK)).isTrue()
    assertThat(userPersistence.writeAuthUser(user.id, authUserId, AuthProvider.KEYCLOAK)).isTrue()

    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isEqualTo(1)
    assertThat(userPersistence.getUserByAuthId(authUserId).orElseThrow().userId).isEqualTo(user.id)
  }

  @Test
  fun `normal authentication replacement rejects a raw subject owned by another User across providers`() {
    val owner = userRepository.save(ScimAirbyteUser(name = "Identity Owner", email = "replacement-owner@example.com"))
    val target = userRepository.save(ScimAirbyteUser(name = "Replacement Target", email = "replacement-target@example.com"))
    val authUserId = "replacement-owned-auth-user"
    val targetAuthUserId = "replacement-target-auth-user"
    userPersistence.writeAuthUser(owner.id, authUserId, AuthProvider.KEYCLOAK)
    userPersistence.writeAuthUser(target.id, targetAuthUserId, AuthProvider.KEYCLOAK)

    assertThat(
      userPersistence.replaceAuthUserForUserId(
        target.id,
        authUserId,
        AuthProvider.GOOGLE_IDENTITY_PLATFORM,
      ),
    ).isFalse()

    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isEqualTo(1)
    assertThat(userPersistence.getUserByAuthId(authUserId).orElseThrow().userId).isEqualTo(owner.id)
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(targetAuthUserId))).isEqualTo(1)
    assertThat(userPersistence.getUserByAuthId(targetAuthUserId).orElseThrow().userId).isEqualTo(target.id)
  }

  @Test
  fun `concurrent different mapped Users cannot claim the same authentication identity`() {
    val tenantA = tenant("first-login-race-a")
    val tenantB = tenant("first-login-race-b")
    val emailA = "identity-race-a@example.com"
    val emailB = "identity-race-b@example.com"
    val createdA =
      mutationService.execute(tenantA.context) {
        lifecycleService.create(tenantA.configurationId, tenantA.organizationId, input(true, userName = emailA))
      }
    val createdB =
      mutationService.execute(tenantB.context) {
        lifecycleService.create(tenantB.configurationId, tenantB.organizationId, input(true, userName = emailB))
      }
    val authUserId = "concurrent-first-login-identity"
    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)

    val futures =
      listOf(emailA, emailB).map { email ->
        executor.submit<ScimFirstLoginAttachmentResult> {
          start.await()
          firstLoginService.attachIfPreProvisioned(email, email, authUserId, AuthProvider.KEYCLOAK)
        }
      }
    start.countDown()
    val results = futures.map { it.get(30, TimeUnit.SECONDS) }
    executor.shutdownNow()

    assertThat(results.filterIsInstance<ScimFirstLoginAttachmentResult.Attached>()).hasSize(1)
    assertThat(results.count { it == ScimFirstLoginAttachmentResult.Conflict }).isEqualTo(1)
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isEqualTo(1)
    val attachedUserId =
      jooq
        .select(Tables.AUTH_USER.USER_ID)
        .from(Tables.AUTH_USER)
        .where(Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))
        .fetchOne(Tables.AUTH_USER.USER_ID)
    assertThat(attachedUserId).isIn(createdA.userId, createdB.userId)
    assertThat(jooq.fetchCount(Tables.USER)).isEqualTo(2)
  }

  @Test
  fun `first-login attachment winning a cross-provider race resolves the normal login to the attached User`() {
    val tenant = tenant("first-login-normal-race")
    val mappedEmail = "first-login-race-mapped@example.com"
    val normalEmail = "first-login-race-normal@example.com"
    val mapped =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true, userName = mappedEmail))
      }
    val authUserId = "first-login-normal-race-subject"
    val ownershipChecked = CountDownLatch(1)
    val releaseAttachment = CountDownLatch(1)
    val pauseFirstOwnershipCheck = AtomicBoolean(true)
    val concurrentAuthUserRepository = spyk(context.getBean(ScimAuthUserRepository::class.java))
    every { concurrentAuthUserRepository.findByAuthUserIdForUpdate(authUserId) } answers {
      callOriginal().also {
        if (pauseFirstOwnershipCheck.compareAndSet(true, false)) {
          check(it.isEmpty())
          ownershipChecked.countDown()
          check(releaseAttachment.await(10, TimeUnit.SECONDS))
        }
      }
    }
    val concurrentFirstLoginService =
      ScimFirstLoginService(
        mappingRepository,
        userRepository,
        concurrentAuthUserRepository,
      )
    val normalUserId = UUID.randomUUID()
    val defaultWorkspaceId = workspace(tenant.organizationId, "first-login-normal-race-workspace")
    val incomingUser =
      AuthenticatedUser()
        .withEmail(normalEmail)
        .withName("Normal Login User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { authenticationResolver.resolveVerifiedEmail() } returns normalEmail
    every { authenticationResolver.resolveRealm() } returns null
    val resourceBootstrapHandler = mockk<ResourceBootstrapHandlerInterface>()
    every { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any()) } returns
      WorkspaceRead().workspaceId(defaultWorkspaceId)
    val handler =
      UserHandler(
        userPersistence,
        mockk<ExternalUserService>(relaxed = true),
        mockk<OrganizationService>(relaxed = true),
        mockk<SsoConfigService>(relaxed = true),
        mockk<OrganizationEmailDomainService>(relaxed = true),
        Optional.empty(),
        mockk<PermissionHandler>(relaxed = true),
        mockk<WorkspacesHandler>(relaxed = true),
        Supplier { normalUserId },
        authenticationResolver,
        Optional.empty<InitialUserConfig>(),
        resourceBootstrapHandler,
        mockk<FeatureFlagClient>(relaxed = true),
        concurrentFirstLoginService,
        transactions,
      )
    val normalCompleted = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val attachmentFuture =
        executor.submit<ScimFirstLoginAttachmentResult> {
          transactions.executeWrite {
            concurrentFirstLoginService.attachIfPreProvisioned(
              mappedEmail,
              mappedEmail,
              authUserId,
              AuthProvider.KEYCLOAK,
            )
          }
        }
      check(ownershipChecked.await(10, TimeUnit.SECONDS))

      val normalLoginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
          }.also { normalCompleted.countDown() }
        }

      assertThat(normalCompleted.await(1, TimeUnit.SECONDS)).isFalse()
      releaseAttachment.countDown()

      assertThat(attachmentFuture.get(30, TimeUnit.SECONDS))
        .isEqualTo(ScimFirstLoginAttachmentResult.Attached(mapped.userId))
      val normalLogin = normalLoginFuture.get(30, TimeUnit.SECONDS).getOrThrow()
      assertThat(normalLogin.newUserCreated).isFalse()
      assertThat(normalLogin.userRead.userId).isEqualTo(mapped.userId)
      assertThat(userPersistence.getUser(normalUserId)).isEmpty()
      assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isEqualTo(1)
      assertThat(userPersistence.getUserByAuthId(authUserId).orElseThrow().userId).isEqualTo(mapped.userId)
    } finally {
      releaseAttachment.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `normal login winning a cross-provider race makes first-login attachment fail closed`() {
    val tenant = tenant("normal-first-login-race")
    val mappedEmail = "normal-first-login-race-mapped@example.com"
    val normalEmail = "normal-first-login-race-normal@example.com"
    val mapped =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input(true, userName = mappedEmail))
      }
    val authUserId = "normal-first-login-race-subject"
    val normalCreateAttempted = CountDownLatch(1)
    val releaseNormalCreate = CountDownLatch(1)
    val concurrentUserPersistence = spyk(userPersistence)
    every { concurrentUserPersistence.createAuthenticatedUserIfNoScimMapping(any(), any()) } answers {
      normalCreateAttempted.countDown()
      check(releaseNormalCreate.await(10, TimeUnit.SECONDS))
      callOriginal()
    }
    val normalUserId = UUID.randomUUID()
    val defaultWorkspaceId = workspace(tenant.organizationId, "normal-first-login-race-workspace")
    val incomingUser =
      AuthenticatedUser()
        .withEmail(normalEmail)
        .withName("Normal Login User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { authenticationResolver.resolveVerifiedEmail() } returns normalEmail
    every { authenticationResolver.resolveRealm() } returns null
    val resourceBootstrapHandler = mockk<ResourceBootstrapHandlerInterface>()
    every { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any()) } returns
      WorkspaceRead().workspaceId(defaultWorkspaceId)
    val handler =
      UserHandler(
        concurrentUserPersistence,
        mockk<ExternalUserService>(relaxed = true),
        mockk<OrganizationService>(relaxed = true),
        mockk<SsoConfigService>(relaxed = true),
        mockk<OrganizationEmailDomainService>(relaxed = true),
        Optional.empty(),
        mockk<PermissionHandler>(relaxed = true),
        mockk<WorkspacesHandler>(relaxed = true),
        Supplier { normalUserId },
        authenticationResolver,
        Optional.empty<InitialUserConfig>(),
        resourceBootstrapHandler,
        mockk<FeatureFlagClient>(relaxed = true),
        firstLoginService,
        transactions,
      )
    val initialWaiters = advisoryLockWaiterCount()
    val executor = Executors.newFixedThreadPool(2)

    try {
      val normalLoginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
          }
        }
      check(normalCreateAttempted.await(10, TimeUnit.SECONDS))

      val attachmentFuture =
        executor.submit<ScimFirstLoginAttachmentResult> {
          firstLoginService.attachIfPreProvisioned(
            mappedEmail,
            mappedEmail,
            authUserId,
            AuthProvider.KEYCLOAK,
          )
        }
      check(waitForAdvisoryLockWaiters(initialWaiters + 1))
      releaseNormalCreate.countDown()

      val normalResult = normalLoginFuture.get(30, TimeUnit.SECONDS).getOrThrow()
      assertThat(normalResult.newUserCreated).isTrue()
      assertThat(normalResult.userRead.userId).isEqualTo(normalUserId)
      assertThat(attachmentFuture.get(30, TimeUnit.SECONDS)).isEqualTo(ScimFirstLoginAttachmentResult.Conflict)
      assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isEqualTo(1)
      assertThat(userPersistence.getUserByAuthId(authUserId).orElseThrow().userId).isEqualTo(normalUserId)
      assertThat(userRepository.findById(mapped.userId)).isPresent
    } finally {
      releaseNormalCreate.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent SCIM POST cannot interleave between NoMatch and orphan relink`() {
    val tenant = tenant("orphan-relink-post-race")
    val email = "orphan-relink-post-race@example.com"
    val existingUser = ordinaryUser(email)
    userPersistence.writeUser(existingUser)
    val authUserId = "orphan-relink-post-race-subject"
    val decisionComplete = CountDownLatch(1)
    val releaseFallback = CountDownLatch(1)
    val attachmentCalls = AtomicInteger()
    val concurrentFirstLoginService =
      spyk(
        ScimFirstLoginService(
          mappingRepository,
          userRepository,
          context.getBean(ScimAuthUserRepository::class.java),
        ),
      )
    every {
      concurrentFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
      )
    } answers {
      callOriginal().also {
        if (attachmentCalls.getAndIncrement() == 0) {
          check(it == ScimFirstLoginAttachmentResult.NoMatch)
          decisionComplete.countDown()
          check(releaseFallback.await(10, TimeUnit.SECONDS))
        }
      }
    }
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("Orphan User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns null
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        attachmentService = concurrentFirstLoginService,
      )
    val initialWaiters = advisoryLockWaiterCount()
    val executor = Executors.newFixedThreadPool(2)

    try {
      val loginFuture =
        executor.submit<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse> {
          handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
        }
      check(decisionComplete.await(10, TimeUnit.SECONDS))

      val scimFuture =
        executor.submit<io.airbyte.domain.models.scim.ScimUserRead> {
          mutationService.execute(tenant.context) {
            lifecycleService.create(
              tenant.configurationId,
              tenant.organizationId,
              input(true, email, "orphan-relink-post-race", "Orphan User"),
            )
          }
        }
      check(waitForAdvisoryLockWaiters(initialWaiters + 1))

      assertThat(scimFuture.isDone).isFalse()
      assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))).isZero()
      releaseFallback.countDown()

      val login = loginFuture.get(30, TimeUnit.SECONDS)
      val mapping = scimFuture.get(30, TimeUnit.SECONDS)
      assertThat(login.newUserCreated).isFalse()
      assertThat(login.userRead.userId).isEqualTo(existingUser.userId)
      assertThat(mapping.userId).isEqualTo(existingUser.userId)
      assertThat(userPersistence.getUserByAuthId(authUserId).orElseThrow().userId).isEqualTo(existingUser.userId)
      assertThat(jooq.fetchCount(Tables.USER, Tables.USER.EMAIL.equalIgnoreCase(email))).isEqualTo(1)
    } finally {
      releaseFallback.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `orphan relink revokes existing and concurrent applications before old subject reassignment`() {
    val email = "orphan-relink-application-race@example.com"
    val existingUser = ordinaryUser(email)
    val oldAuthUserId = "orphan-relink-application-old"
    val incomingAuthUserId = "orphan-relink-application-incoming"
    userPersistence.writeUser(existingUser)
    userPersistence.writeAuthUser(existingUser.userId, oldAuthUserId, AuthProvider.KEYCLOAK)
    val oldAuthedUser =
      io.airbyte.config.helpers.AuthenticatedUserConverter.toAuthenticatedUser(
        existingUser,
        oldAuthUserId,
        AuthProvider.KEYCLOAK,
      )
    val existingApplication = applicationService.createApplication(oldAuthedUser, "Existing Orphan Application")

    val applicationCreationHasIdentityLock = CountDownLatch(1)
    val releaseApplicationCreation = CountDownLatch(1)
    val pauseFirstApplicationOperation = AtomicBoolean(true)
    val concurrentOwnershipService =
      object : ScimAuthUserOwnershipService(context.getBean(ScimAuthUserRepository::class.java)) {
        override fun <T> withUniqueOwner(
          authUserId: String,
          expectedUserId: UUID?,
          operation: () -> T,
        ): T =
          super.withUniqueOwner(authUserId, expectedUserId) {
            if (authUserId == oldAuthUserId && pauseFirstApplicationOperation.compareAndSet(true, false)) {
              applicationCreationHasIdentityLock.countDown()
              check(releaseApplicationCreation.await(10, TimeUnit.SECONDS))
            }
            operation()
          }
      }
    val concurrentApplicationService =
      ApplicationServiceDataImpl(
        context.getBean(ApplicationRepository::class.java),
        context.getBean(AirbyteAuthConfig::class.java),
        jwtTokenGenerator,
        concurrentOwnershipService,
      )
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("Orphan Relink Application")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns email
    every { authenticationResolver.resolveRealm() } returns null
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(oldAuthUserId) } returns null
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        applicationService = Optional.of(concurrentApplicationService),
      )
    val executor = Executors.newFixedThreadPool(2)

    try {
      val applicationFuture =
        executor.submit<io.airbyte.config.Application> {
          transactions.executeWrite {
            concurrentApplicationService.createApplication(oldAuthedUser, "Concurrent Orphan Application")
          }
        }
      check(applicationCreationHasIdentityLock.await(10, TimeUnit.SECONDS))

      val loginFuture =
        executor.submit<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse> {
          handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
        }
      assertThat(loginFuture.isDone).isFalse()

      releaseApplicationCreation.countDown()
      val concurrentApplication = applicationFuture.get(30, TimeUnit.SECONDS)
      val login = loginFuture.get(30, TimeUnit.SECONDS)

      assertThat(login.userRead.userId).isEqualTo(existingUser.userId)
      assertThat(jooq.fetchCount(Tables.APPLICATION, Tables.APPLICATION.AUTH_USER_ID.eq(oldAuthUserId))).isZero()
      assertThat(userPersistence.getUserByAuthId(oldAuthUserId)).isEmpty()
      assertThat(userPersistence.getUserByAuthId(incomingAuthUserId).orElseThrow().userId).isEqualTo(existingUser.userId)

      val laterOwner = ordinaryUser("orphan-relink-later-owner@example.com")
      userPersistence.writeUser(laterOwner)
      assertThat(userPersistence.writeAuthUser(laterOwner.userId, oldAuthUserId, AuthProvider.KEYCLOAK)).isTrue()
      assertThatThrownBy {
        concurrentApplicationService.getToken(existingApplication.clientId, existingApplication.clientSecret)
      }.isInstanceOf(IllegalArgumentException::class.java)
      assertThatThrownBy {
        concurrentApplicationService.getToken(concurrentApplication.clientId, concurrentApplication.clientSecret)
      }.isInstanceOf(IllegalArgumentException::class.java)
    } finally {
      releaseApplicationCreation.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `SCIM POST winning the email lock prevents default User clearing and login side effects`() {
    val tenant = tenant("default-user-post-race")
    val email = "default-user-post-race@example.com"
    userPersistence.writeUser(ordinaryUser(email).withUserId(io.airbyte.commons.DEFAULT_USER_ID))
    val mappingHasLock = CountDownLatch(1)
    val releaseMapping = CountDownLatch(1)
    val concurrentUserRepository = spyk(userRepository)
    every { concurrentUserRepository.acquireGlobalEmailLock(email) } answers {
      callOriginal().also {
        mappingHasLock.countDown()
        check(releaseMapping.await(10, TimeUnit.SECONDS))
      }
    }
    val concurrentLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        concurrentUserRepository,
        permissionRepository,
        groupMemberRepository,
        domainVerificationRepository,
      )
    val authUserId = "default-user-post-race-subject"
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("Default User Login")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns null
    every { authenticationResolver.resolveRealm() } returns null
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
      )
    val initialWaiters = advisoryLockWaiterCount()
    val executor = Executors.newFixedThreadPool(2)

    try {
      val scimFuture =
        executor.submit<io.airbyte.domain.models.scim.ScimUserRead> {
          mutationService.execute(tenant.context) {
            concurrentLifecycle.create(
              tenant.configurationId,
              tenant.organizationId,
              input(true, email, "default-user-post-race", "Default User"),
            )
          }
        }
      check(mappingHasLock.await(10, TimeUnit.SECONDS))

      val loginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
          }
        }
      check(waitForAdvisoryLockWaiters(initialWaiters + 1))

      assertThat(loginFuture.isDone).isFalse()
      assertThat(userPersistence.getUser(io.airbyte.commons.DEFAULT_USER_ID).orElseThrow().email).isEqualTo(email)
      assertThat(jooq.fetchCount(Tables.AUTH_USER)).isZero()
      releaseMapping.countDown()

      val mapping = scimFuture.get(30, TimeUnit.SECONDS)
      val login = loginFuture.get(30, TimeUnit.SECONDS)
      assertThat(login.exceptionOrNull())
        .isInstanceOf(io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem::class.java)
      assertThat(mapping.userId).isEqualTo(io.airbyte.commons.DEFAULT_USER_ID)
      assertThat(userPersistence.getUser(io.airbyte.commons.DEFAULT_USER_ID).orElseThrow().email).isEqualTo(email)
      assertThat(jooq.fetchCount(Tables.USER)).isEqualTo(1)
      assertThat(jooq.fetchCount(Tables.AUTH_USER)).isZero()
    } finally {
      releaseMapping.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent mapping email transition cannot interleave before SSO migration side effects`() {
    val tenant = tenant("sso-transition-race")
    val loginEmail = "sso-transition-stale-global@example.com"
    val currentMappingEmail = "sso-transition-current@example.com"
    val mapped =
      mutationService.execute(tenant.context) {
        lifecycleService.create(
          tenant.configurationId,
          tenant.organizationId,
          input(true, loginEmail, "sso-transition-race", "Mapped User"),
        )
      }
    mutationService.execute(tenant.context) {
      lifecycleService.replace(
        tenant.configurationId,
        tenant.organizationId,
        mapped.id,
        input(true, currentMappingEmail, "sso-transition-race", "Mapped User"),
      )
    }
    val existingAuthUserId = "sso-transition-existing-subject"
    userPersistence.writeAuthUser(mapped.userId, existingAuthUserId, AuthProvider.KEYCLOAK)
    val incomingAuthUserId = "sso-transition-incoming-subject"
    val decisionComplete = CountDownLatch(1)
    val releaseFallback = CountDownLatch(1)
    val concurrentFirstLoginService =
      spyk(
        ScimFirstLoginService(
          mappingRepository,
          userRepository,
          context.getBean(ScimAuthUserRepository::class.java),
        ),
      )
    every {
      concurrentFirstLoginService.attachIfPreProvisioned(
        loginEmail,
        loginEmail,
        incomingAuthUserId,
        AuthProvider.KEYCLOAK,
        tenant.organizationId,
      )
    } answers {
      callOriginal().also {
        check(it == ScimFirstLoginAttachmentResult.NoMatch)
        decisionComplete.countDown()
        check(releaseFallback.await(10, TimeUnit.SECONDS))
      }
    }
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(incomingAuthUserId) } returns
      AuthenticatedUser()
        .withEmail(loginEmail)
        .withName("Mapped User")
        .withAuthUserId(incomingAuthUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { authenticationResolver.resolveVerifiedEmail() } returns loginEmail
    every { authenticationResolver.resolveRealm() } returns "new-sso-realm"
    val organizationService = mockk<OrganizationService>(relaxed = true)
    every { organizationService.getOrganizationBySsoConfigRealm("new-sso-realm") } returns
      Optional.of(
        io.airbyte.config
          .Organization()
          .withOrganizationId(tenant.organizationId),
      )
    val externalUserService = mockk<ExternalUserService>(relaxed = true)
    every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns "legacy-realm"
    val featureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
    every {
      featureFlagClient.boolVariation(
        io.airbyte.featureflag.BypassSsoDomainValidationEnforcement,
        any(),
      )
    } returns true
    val handler =
      loginHandler(
        authenticationResolver = authenticationResolver,
        userId = UUID.randomUUID(),
        externalUserService = externalUserService,
        organizationService = organizationService,
        featureFlagClient = featureFlagClient,
        attachmentService = concurrentFirstLoginService,
      )
    val initialWaiters = lockWaiterCount()
    val executor = Executors.newFixedThreadPool(2)

    try {
      val loginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(incomingAuthUserId))
          }
        }
      check(decisionComplete.await(10, TimeUnit.SECONDS))

      val transitionFuture =
        executor.submit<io.airbyte.domain.models.scim.ScimUserRead> {
          mutationService.execute(tenant.context) {
            lifecycleService.replace(
              tenant.configurationId,
              tenant.organizationId,
              mapped.id,
              input(true, loginEmail, "sso-transition-race", "Mapped User"),
            )
          }
        }
      check(waitForLockWaiters(initialWaiters + 1))

      assertThat(transitionFuture.isDone).isFalse()
      verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) }
      releaseFallback.countDown()

      val login = loginFuture.get(30, TimeUnit.SECONDS)
      val transitioned = transitionFuture.get(30, TimeUnit.SECONDS)
      assertThat(login.exceptionOrNull())
        .isInstanceOf(io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem::class.java)
      assertThat(transitioned.primaryEmail).isEqualTo(loginEmail)
      verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) }
      assertThat(userPersistence.getUserByAuthId(existingAuthUserId).orElseThrow().userId).isEqualTo(mapped.userId)
      assertThat(userPersistence.getUserByAuthId(incomingAuthUserId)).isEmpty()
    } finally {
      releaseFallback.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `email-shaped identity cannot deadlock with a reversed global email transition`() {
    val tenant = tenant("email-identity-lock-domain")
    val originalMappedEmail = "middle@example.com"
    val targetMappedEmail = "zulu@example.com"
    val authUserId = "alpha@example.com"
    val mapped =
      mutationService.execute(tenant.context) {
        lifecycleService.create(
          tenant.configurationId,
          tenant.organizationId,
          input(true, userName = originalMappedEmail),
        )
      }
    mutationService.execute(tenant.context) {
      lifecycleService.replace(
        tenant.configurationId,
        tenant.organizationId,
        mapped.id,
        input(true, userName = targetMappedEmail),
      )
    }
    val ordinaryUser = ordinaryUser(authUserId)
    userPersistence.writeUser(ordinaryUser)
    val identityLockAttempted = CountDownLatch(1)
    val releaseIdentityLock = CountDownLatch(1)
    val concurrentAuthUserRepository = spyk(context.getBean(ScimAuthUserRepository::class.java))
    every { concurrentAuthUserRepository.acquireIdentityLock(authUserId) } answers {
      identityLockAttempted.countDown()
      check(releaseIdentityLock.await(10, TimeUnit.SECONDS))
      callOriginal()
    }
    val concurrentFirstLoginService =
      ScimFirstLoginService(
        mappingRepository,
        userRepository,
        concurrentAuthUserRepository,
      )
    val initialWaiters = advisoryLockWaiterCount()
    val executor = Executors.newFixedThreadPool(2)

    try {
      val attachmentFuture =
        executor.submit<Result<ScimFirstLoginAttachmentResult>> {
          runCatching {
            transactions.executeWrite {
              concurrentFirstLoginService.attachIfPreProvisioned(
                targetMappedEmail,
                targetMappedEmail,
                authUserId,
                AuthProvider.KEYCLOAK,
              )
            }
          }
        }
      check(identityLockAttempted.await(10, TimeUnit.SECONDS))

      val emailUpdateFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            userPersistence.writeUser(ordinaryUser.withEmail(targetMappedEmail))
          }
        }
      check(waitForAdvisoryLockWaiters(initialWaiters + 1))
      releaseIdentityLock.countDown()

      assertThat(attachmentFuture.get(30, TimeUnit.SECONDS).getOrThrow())
        .isEqualTo(ScimFirstLoginAttachmentResult.Attached(mapped.userId))
      assertThat(emailUpdateFuture.get(30, TimeUnit.SECONDS).isSuccess).isTrue()
      assertThat(userPersistence.getUser(ordinaryUser.userId).orElseThrow().email).isEqualTo(targetMappedEmail)
    } finally {
      releaseIdentityLock.countDown()
      executor.shutdownNow()
    }
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
        domainVerificationRepository,
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
        domainVerificationRepository,
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
        domainVerificationRepository,
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
        domainVerificationRepository,
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
  fun `full login racing a mapping email transition cannot create an identity for a different User`() {
    val tenant = tenant("first-login-email-transition")
    val oldEmail = "transition-old@example.com"
    val loginEmail = "transition-current@example.com"
    val mapped =
      mutationService.execute(tenant.context) {
        lifecycleService.create(
          tenant.configurationId,
          tenant.organizationId,
          input(true, oldEmail, "transition-external", "Mapped User"),
        )
      }
    val defaultWorkspaceId = workspace(tenant.organizationId, "transition-workspace")
    val lifecycleReachedTargetEmailLock = CountDownLatch(1)
    val allowLifecycleTargetEmailLock = CountDownLatch(1)
    val concurrentUserRepository = spyk(userRepository)
    every { concurrentUserRepository.acquireGlobalEmailLock(loginEmail) } answers {
      callOriginal().also {
        lifecycleReachedTargetEmailLock.countDown()
        check(allowLifecycleTargetEmailLock.await(10, TimeUnit.SECONDS))
      }
    }
    val concurrentLifecycle =
      ScimUserLifecycleService(
        mappingRepository,
        concurrentUserRepository,
        permissionRepository,
        groupMemberRepository,
        domainVerificationRepository,
      )
    val authUserId = "transition-login-auth-user"
    val incomingUser =
      AuthenticatedUser()
        .withEmail(loginEmail)
        .withName("Login User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    val authenticationResolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { authenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { authenticationResolver.resolveVerifiedEmail() } returns loginEmail
    every { authenticationResolver.resolveRealm() } returns null
    val resourceBootstrapHandler = mockk<ResourceBootstrapHandlerInterface>()
    every { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any()) } returns
      WorkspaceRead().workspaceId(defaultWorkspaceId)
    val loginUserId = UUID.randomUUID()
    val handler =
      UserHandler(
        userPersistence,
        mockk<ExternalUserService>(relaxed = true),
        mockk<OrganizationService>(relaxed = true),
        mockk<SsoConfigService>(relaxed = true),
        mockk<OrganizationEmailDomainService>(relaxed = true),
        Optional.empty(),
        mockk<PermissionHandler>(relaxed = true),
        mockk<WorkspacesHandler>(relaxed = true),
        Supplier { loginUserId },
        authenticationResolver,
        Optional.empty<InitialUserConfig>(),
        resourceBootstrapHandler,
        mockk<FeatureFlagClient>(relaxed = true),
        firstLoginService,
        transactions,
      )
    val executor = Executors.newFixedThreadPool(2)

    try {
      val lifecycleFuture =
        executor.submit<Result<Unit>> {
          runCatching {
            mutationService.execute(tenant.context) {
              concurrentLifecycle.replace(
                tenant.configurationId,
                tenant.organizationId,
                mapped.id,
                input(true, loginEmail, "transition-external", "Mapped User"),
              )
              Unit
            }
          }
        }
      check(lifecycleReachedTargetEmailLock.await(10, TimeUnit.SECONDS))

      val initialWaiters = advisoryLockWaiterCount()
      val loginFuture =
        executor.submit<Result<io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse>> {
          runCatching {
            handler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
          }
        }
      check(waitForAdvisoryLockWaiters(initialWaiters + 1))
      assertThat(loginFuture.isDone).isFalse()
      allowLifecycleTargetEmailLock.countDown()

      val loginResult = loginFuture.get(15, TimeUnit.SECONDS)
      val lifecycleResult = lifecycleFuture.get(15, TimeUnit.SECONDS)

      assertThat(lifecycleResult.isSuccess).isTrue()
      assertThat(loginResult.getOrThrow().newUserCreated).isFalse()
      assertThat(loginResult.getOrThrow().userRead.userId).isEqualTo(mapped.userId)
      assertThat(
        mappingRepository
          .findUser(
            mapped.id,
            tenant.configurationId,
            tenant.organizationId,
          )?.primaryEmail,
      ).isEqualTo(loginEmail)
      assertThat(userPersistence.getUserByAuthId(authUserId).orElseThrow().userId).isEqualTo(mapped.userId)
      assertThat(userPersistence.getUser(loginUserId)).isEmpty()
      assertThat(jooq.fetchCount(Tables.USER, Tables.USER.EMAIL.equalIgnoreCase(loginEmail))).isZero()
    } finally {
      allowLifecycleTargetEmailLock.countDown()
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
        domainVerificationRepository,
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
        domainVerificationRepository,
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
        domainVerificationRepository,
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
        domainVerificationRepository,
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

  private fun loginHandler(
    authenticationResolver: UserAuthenticationResolver,
    userId: UUID,
    persistence: UserPersistence = userPersistence,
    externalUserService: ExternalUserService = mockk(relaxed = true),
    organizationService: OrganizationService = mockk(relaxed = true),
    organizationEmailDomainService: OrganizationEmailDomainService = mockk(relaxed = true),
    ssoConfigService: SsoConfigService = mockk(relaxed = true),
    applicationService: Optional<ApplicationService> = Optional.empty(),
    permissionHandler: PermissionHandler = mockk(relaxed = true),
    workspacesHandler: WorkspacesHandler = mockk(relaxed = true),
    resourceBootstrapHandler: ResourceBootstrapHandlerInterface = mockk(relaxed = true),
    featureFlagClient: FeatureFlagClient = mockk(relaxed = true),
    attachmentService: ScimFirstLoginService = firstLoginService,
    uuidSupplier: Supplier<UUID> = Supplier { userId },
    transactionOperations: TransactionOperations<Connection> = transactions,
  ): UserHandler =
    UserHandler(
      persistence,
      externalUserService,
      organizationService,
      ssoConfigService,
      organizationEmailDomainService,
      applicationService,
      permissionHandler,
      workspacesHandler,
      uuidSupplier,
      authenticationResolver,
      Optional.empty<InitialUserConfig>(),
      resourceBootstrapHandler,
      featureFlagClient,
      attachmentService,
      transactionOperations,
    )

  private fun roleResolver(): RoleResolver =
    RoleResolver(
      context.getBean(AuthenticationHeaderResolver::class.java),
      mockk<CurrentUserService>(),
      null,
      PermissionHandler(null, mockk<WorkspaceService>(), null, permissionService),
    )

  private fun insertAuthUser(
    userId: UUID,
    authUserId: String,
    authProvider: AuthProvider,
  ) {
    jooq
      .insertInto(Tables.AUTH_USER)
      .set(Tables.AUTH_USER.ID, UUID.randomUUID())
      .set(Tables.AUTH_USER.USER_ID, userId)
      .set(Tables.AUTH_USER.AUTH_USER_ID, authUserId)
      .set(
        Tables.AUTH_USER.AUTH_PROVIDER,
        io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider
          .lookupLiteral(authProvider.value()),
      ).execute()
  }

  private fun ordinaryUser(email: String): User =
    User()
      .withUserId(UUID.randomUUID())
      .withName("Ordinary User")
      .withEmail(email)
      .withNews(false)
      .withUiMetadata(objectMapper.createObjectNode())

  private fun advisoryLockWaiterCount(): Int =
    jooq
      .fetchOne(
        """
        SELECT COUNT(*)
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND wait_event_type = 'Lock'
          AND wait_event = 'advisory'
        """,
      )!!
      .get(0, Int::class.java)

  private fun lockWaiterCount(): Int =
    jooq
      .fetchOne(
        """
        SELECT COUNT(*)
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND wait_event_type = 'Lock'
        """,
      )!!
      .get(0, Int::class.java)

  private fun waitForAdvisoryLockWaiters(expected: Int): Boolean {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (System.nanoTime() < deadline) {
      if (advisoryLockWaiterCount() >= expected) {
        return true
      }
      Thread.sleep(20)
    }
    return false
  }

  private fun waitForLockWaiters(expected: Int): Boolean {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (System.nanoTime() < deadline) {
      if (lockWaiterCount() >= expected) {
        return true
      }
      Thread.sleep(20)
    }
    return false
  }

  /**
   * SCIM provisioning is gated on verified domain ownership, so every tenant that provisions users
   * needs a `verified` domain record for the domains its fixtures use.
   */
  private fun tenant(
    name: String,
    verifiedDomains: List<String> = listOf("example.com"),
  ): Tenant {
    val organization = organizationRepository.save(Organization(name = name, email = "$name@example.com"))
    verifiedDomains.forEach { verifyDomain(organization.id!!, it) }
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
    private lateinit var domainVerificationRepository: OrganizationDomainVerificationRepository
    private lateinit var userInvitationRepository: UserInvitationRepository
    private lateinit var applicationService: ApplicationService
    private lateinit var permissionService: PermissionService
    private lateinit var groupService: GroupService
    private lateinit var lifecycleService: ScimUserLifecycleService
    private lateinit var firstLoginService: ScimFirstLoginService
    private lateinit var mutationService: ScimMutationService
    private lateinit var userPersistence: UserPersistence
    private lateinit var permissionPersistence: PermissionPersistence
    private lateinit var dataSource: DataSource
    private lateinit var database: Database
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
      dataSource =
        (context.getBean(DataSource::class.java, Qualifiers.byName("config")) as DelegatingDataSource)
          .targetDataSource
      jooq = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
      database = TestDatabaseProviders(dataSource, jooq).createNewConfigsDatabase()
      userPersistence = UserPersistence(database)
      permissionPersistence = PermissionPersistence(database)

      objectMapper = context.getBean(ObjectMapper::class.java)
      organizationRepository = context.getBean(OrganizationRepository::class.java)
      configurationRepository = context.getBean(ScimConfigurationRepository::class.java)
      mappingRepository = context.getBean(ScimResourceMappingRepository::class.java)
      userRepository = context.getBean(ScimAirbyteUserRepository::class.java)
      permissionRepository = context.getBean(PermissionRepository::class.java)
      groupMemberRepository = context.getBean(GroupMemberRepository::class.java)
      domainVerificationRepository = context.getBean(OrganizationDomainVerificationRepository::class.java)
      userInvitationRepository = context.getBean(UserInvitationRepository::class.java)
      permissionService = context.getBean(PermissionService::class.java)
      groupService = context.getBean(GroupService::class.java)
      jwtTokenGenerator = context.getBean(JwtTokenGenerator::class.java)
      applicationService =
        ApplicationServiceDataImpl(
          context.getBean(ApplicationRepository::class.java),
          context.getBean(AirbyteAuthConfig::class.java),
          jwtTokenGenerator,
          context.getBean(ScimAuthUserOwnershipService::class.java),
        )
      @Suppress("UNCHECKED_CAST")
      jwtTokenValidator =
        context.getBean(ReactiveJsonWebTokenValidator::class.java) as ReactiveJsonWebTokenValidator<*, HttpRequest<*>>
      @Suppress("UNCHECKED_CAST")
      transactions =
        context.getBean(TransactionOperations::class.java, Qualifiers.byName("config")) as TransactionOperations<Connection>
      lifecycleService =
        ScimUserLifecycleService(
          mappingRepository,
          userRepository,
          permissionRepository,
          groupMemberRepository,
          domainVerificationRepository,
        )
      firstLoginService = context.getBean(ScimFirstLoginService::class.java)
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
