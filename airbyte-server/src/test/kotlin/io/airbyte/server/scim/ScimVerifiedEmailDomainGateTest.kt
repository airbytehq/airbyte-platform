/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.UserAuthIdRequestBody
import io.airbyte.commons.auth.config.InitialUserConfig
import io.airbyte.commons.auth.support.UserAuthenticationResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.ResourceBootstrapHandlerInterface
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.OrganizationDomainVerificationRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.OrganizationDomainVerification
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.ScimAirbyteUser
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.data.services.ExternalUserService
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationMethod
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimEmailDomainNotVerifiedException
import io.airbyte.domain.models.scim.ScimUserWrite
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimFirstLoginAttachmentResult
import io.airbyte.domain.services.scim.ScimFirstLoginService
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.domain.services.scim.ScimUserLifecycleService
import io.airbyte.featureflag.FeatureFlagClient
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.transaction.TransactionOperations
import io.mockk.every
import io.mockk.mockk
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
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import javax.sql.DataSource

/**
 * SCIM provisioning is gated on verified domain ownership: an organization may only name an email
 * address whose domain it holds a `verified` `organization_domain_verification` record for.
 *
 * Without that gate, any SCIM-entitled organization could `POST /Users` with an arbitrary victim's
 * address, have [ScimUserLifecycleService.create] reuse the victim's existing global User row, then
 * repoint the mapping's email to an address the attacker controls. First login on that address
 * bound the attacker's auth subject to the victim's User, inheriting every permission the victim
 * held in every organization.
 *
 * These tests exercise the real Postgres schema because the gate's correctness lives in SQL: the
 * `status = verified` predicate, the case-insensitive domain match, and the tombstone filter.
 */
class ScimVerifiedEmailDomainGateTest {
  @AfterEach
  fun cleanUp() {
    jooq.deleteFrom(Tables.GROUP_MEMBER).execute()
    jooq.deleteFrom(Tables.PERMISSION).execute()
    jooq.deleteFrom(Tables.SCIM_RESOURCE_MAPPING).execute()
    jooq.deleteFrom(Tables.SCIM_CONFIGURATION).execute()
    jooq.deleteFrom(Tables.ORGANIZATION_DOMAIN_VERIFICATION).execute()
    jooq.deleteFrom(Tables.ORGANIZATION).execute()
    jooq.deleteFrom(Tables.AUTH_USER).execute()
    jooq.deleteFrom(Tables.USER).execute()
  }

  // --------------------------------------------------------------------------
  // The attack, blocked at step 1.
  // --------------------------------------------------------------------------

  @Test
  fun `an organization cannot provision a User on a domain it has not verified`() {
    val victim = provisionVictim()
    val attacker = tenant("attacker-blocked", listOf("evil.example"))

    assertThatThrownBy {
      mutationService.execute(attacker.context) {
        lifecycleService.create(attacker.configurationId, attacker.organizationId, input(VICTIM_EMAIL, "ext-attack"))
      }
    }.isInstanceOf(ScimEmailDomainNotVerifiedException::class.java)

    // Step 1 never lands: no mapping exists, so nothing can point at the victim's User.
    assertThat(jooq.fetchCount(Tables.SCIM_RESOURCE_MAPPING)).isZero()

    // And the payoff is unreachable: first login on the attacker's address finds nothing to attach
    // to, where before the gate it returned Attached(victimUserId).
    val subject = "attacker-blocked-subject"
    val attachment = firstLoginService.attachIfPreProvisioned(ATTACKER_EMAIL, ATTACKER_EMAIL, subject, AuthProvider.KEYCLOAK)

    assertThat(attachment).isEqualTo(ScimFirstLoginAttachmentResult.NoMatch)
    assertThat(jooq.fetchCount(Tables.AUTH_USER, Tables.AUTH_USER.AUTH_USER_ID.eq(subject))).isZero()
    assertThat(hasVictimAdmin(subject, victim.orgId)).isFalse()
  }

  @Test
  fun `an organization cannot repoint an existing mapping onto a domain it has not verified`() {
    val attacker = tenant("attacker-repoint", listOf("evil.example"))
    val created =
      mutationService.execute(attacker.context) {
        lifecycleService.create(attacker.configurationId, attacker.organizationId, input("bot@evil.example", "ext-repoint"))
      }

    // Squatting an address no global User owns yet is the variant the ownership-transition guard
    // cannot see, because there is no conflicting global User to compare against.
    assertThatThrownBy {
      mutationService.execute(attacker.context) {
        lifecycleService.replace(
          attacker.configurationId,
          attacker.organizationId,
          created.id,
          input("future-hire@victim.com", "ext-repoint"),
        )
      }
    }.isInstanceOf(ScimEmailDomainNotVerifiedException::class.java)

    val mapping = mappingRepository.findUser(created.id, attacker.configurationId, attacker.organizationId)
    assertThat(mapping?.primaryEmail).isEqualTo("bot@evil.example")

    // The squatted address stays unclaimed, so the real owner's first login attaches to nothing
    // rather than landing inside the attacker's User.
    val subject = "future-hire-subject"
    val attachment =
      firstLoginService.attachIfPreProvisioned(
        "future-hire@victim.com",
        "future-hire@victim.com",
        subject,
        AuthProvider.KEYCLOAK,
      )

    assertThat(attachment).isEqualTo(ScimFirstLoginAttachmentResult.NoMatch)
  }

  @Test
  fun `PATCH is gated on the same authority as PUT`() {
    val attacker = tenant("attacker-patch", listOf("evil.example"))
    val created =
      mutationService.execute(attacker.context) {
        lifecycleService.create(attacker.configurationId, attacker.organizationId, input("bot@evil.example", "ext-patch"))
      }

    assertThatThrownBy {
      mutationService.execute(attacker.context) {
        lifecycleService.patch(
          attacker.configurationId,
          attacker.organizationId,
          created.id,
          input("future-hire@victim.com", "ext-patch"),
          emptyList(),
        )
      }
    }.isInstanceOf(ScimEmailDomainNotVerifiedException::class.java)

    assertThat(
      mappingRepository.findUser(created.id, attacker.configurationId, attacker.organizationId)?.primaryEmail,
    ).isEqualTo("bot@evil.example")
  }

  // --------------------------------------------------------------------------
  // PLAT-941's specified behaviour must survive the gate.
  // --------------------------------------------------------------------------

  @Test
  fun `PLAT-941 an email change before first login still attaches the login to the mapped User`() {
    val tenant = tenant("plat-941-regression", listOf("corp.example", "corp-alias.example"))
    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("alice@corp.example", "ext-941"))
      }

    val updated =
      mutationService.execute(tenant.context) {
        lifecycleService.replace(
          tenant.configurationId,
          tenant.organizationId,
          created.id,
          input("alice.newname@corp-alias.example", "ext-941"),
        )
      }

    // The mapping's email moved; ownership stayed with the User the mapping was provisioned for.
    assertThat(updated.primaryEmail).isEqualTo("alice.newname@corp-alias.example")
    assertThat(updated.userId).isEqualTo(created.userId)

    // First login matches the mapping's *current* primary_email, not the stale one.
    val subject = "plat-941-subject"
    val response =
      loginHandler(resolver(subject, "alice.newname@corp-alias.example"))
        .getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(subject))

    assertThat(response.userRead.userId).isEqualTo(created.userId)
    assertThat(
      permissionService.getPermissionsByAuthUserId(subject).map { it.organizationId },
    ).contains(tenant.organizationId)
  }

  @Test
  fun `a verified domain lets an organization provision and reuse an existing global User on that domain`() {
    val colleague = userRepository.save(ScimAirbyteUser(name = "Colleague", email = "colleague@corp.example"))
    val tenant = tenant("verified-reuse", listOf("corp.example"))

    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("colleague@corp.example", "ext-reuse"))
      }

    assertThat(created.userId).isEqualTo(colleague.id)
  }

  // --------------------------------------------------------------------------
  // Which records count as authority.
  // --------------------------------------------------------------------------

  @Test
  fun `only VERIFIED records grant authority`() {
    listOf(
      DomainVerificationStatus.pending,
      DomainVerificationStatus.failed,
      DomainVerificationStatus.expired,
    ).forEachIndexed { index, status ->
      val tenant = tenant("unverified-status-$index", emptyList())
      saveVerification(tenant.organizationId, "corp.example", status = status)

      assertThatThrownBy {
        mutationService.execute(tenant.context) {
          lifecycleService.create(tenant.configurationId, tenant.organizationId, input("alice@corp.example", "ext-$index"))
        }
      }.describedAs("status %s must not grant provisioning authority", status)
        .isInstanceOf(ScimEmailDomainNotVerifiedException::class.java)
    }
  }

  @Test
  fun `a verified record with no verified_at timestamp still grants authority`() {
    val tenant = tenant("verified-without-timestamp", emptyList())
    saveVerification(tenant.organizationId, "corp.example", verifiedAt = null)

    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("alice@corp.example", "ext-no-ts"))
      }

    assertThat(created.primaryEmail).isEqualTo("alice@corp.example")
  }

  @Test
  fun `a tombstoned verified record does not grant authority`() {
    val tenant = tenant("tombstoned-verification", emptyList())
    saveVerification(tenant.organizationId, "corp.example", tombstone = true)

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("alice@corp.example", "ext-tombstone"))
      }
    }.isInstanceOf(ScimEmailDomainNotVerifiedException::class.java)
  }

  @Test
  fun `an organization with no domain records at all cannot provision`() {
    val tenant = tenant("no-verifications", emptyList())

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("alice@corp.example", "ext-none"))
      }
    }.isInstanceOf(ScimEmailDomainNotVerifiedException::class.java)
  }

  @Test
  fun `another organization's verified record does not grant authority`() {
    val owner = tenant("domain-owner", listOf("corp.example"))
    val outsider = tenant("domain-outsider", emptyList())
    assertThat(owner.organizationId).isNotEqualTo(outsider.organizationId)

    assertThatThrownBy {
      mutationService.execute(outsider.context) {
        lifecycleService.create(outsider.configurationId, outsider.organizationId, input("alice@corp.example", "ext-outsider"))
      }
    }.isInstanceOf(ScimEmailDomainNotVerifiedException::class.java)
  }

  @Test
  fun `domain ownership matches case insensitively in both directions`() {
    val tenant = tenant("case-insensitive", emptyList())
    saveVerification(tenant.organizationId, "Corp.Example")

    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("Alice@CORP.EXAMPLE", "ext-case"))
      }

    assertThat(created.userId).isNotNull()
  }

  @Test
  fun `addresses with no parseable domain are rejected`() {
    val tenant = tenant("malformed-email", listOf("corp.example"))

    listOf("no-at-sign", "alice@", "", "alice@   ").forEachIndexed { index, malformed ->
      assertThatThrownBy {
        mutationService.execute(tenant.context) {
          lifecycleService.create(tenant.configurationId, tenant.organizationId, input(malformed, "ext-malformed-$index"))
        }
      }.describedAs("address %s must be rejected", malformed)
        .isInstanceOf(ScimEmailDomainNotVerifiedException::class.java)
    }

    assertThat(jooq.fetchCount(Tables.SCIM_RESOURCE_MAPPING)).isZero()
  }

  // ==========================================================================
  // Helpers
  // ==========================================================================

  private data class Victim(
    val userId: UUID,
    val orgId: UUID,
  )

  private data class Tenant(
    val organizationId: UUID,
    val configurationId: UUID,
    val context: ScimAuthenticationContext,
  )

  /** A pre-existing global User who is organization_admin in their own organization. */
  private fun provisionVictim(): Victim {
    val victimOrg = organizationRepository.save(Organization(name = "victim-org", email = "victim-org@example.com"))
    val victim = userRepository.save(ScimAirbyteUser(name = "Victim", email = VICTIM_EMAIL))
    permissionRepository.save(
      Permission(
        userId = victim.id,
        organizationId = victimOrg.id,
        permissionType = PermissionType.organization_admin,
      ),
    )
    return Victim(userId = victim.id!!, orgId = victimOrg.id!!)
  }

  private fun tenant(
    name: String,
    verifiedDomains: List<String>,
  ): Tenant {
    val organization = organizationRepository.save(Organization(name = name, email = "$name@example.com"))
    verifiedDomains.forEach { saveVerification(organization.id!!, it) }
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

  private fun saveVerification(
    organizationId: UUID,
    domain: String,
    status: DomainVerificationStatus = DomainVerificationStatus.verified,
    tombstone: Boolean = false,
    verifiedAt: OffsetDateTime? = OffsetDateTime.now(),
  ) {
    domainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = organizationId,
        domain = domain,
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = status,
        verificationToken = UUID.randomUUID().toString(),
        dnsRecordName = "_airbyte-verification.$domain",
        dnsRecordPrefix = "_airbyte-verification",
        verifiedAt = verifiedAt,
        tombstone = tombstone,
      ),
    )
  }

  private fun hasVictimAdmin(
    subject: String,
    victimOrgId: UUID,
  ): Boolean =
    permissionService.getPermissionsByAuthUserId(subject).any {
      it.organizationId == victimOrgId &&
        it.permissionType == io.airbyte.config.Permission.PermissionType.ORGANIZATION_ADMIN
    }

  private fun resolver(
    subject: String,
    email: String,
  ): UserAuthenticationResolver {
    val resolver = mockk<UserAuthenticationResolver>(relaxed = true)
    every { resolver.resolveUser(subject) } returns
      AuthenticatedUser()
        .withEmail(email)
        .withName("Login User")
        .withAuthUserId(subject)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { resolver.resolveVerifiedEmail() } returns email
    every { resolver.resolveRealm() } returns null
    return resolver
  }

  /** UserHandler whose only real collaborators are firstLoginService, userPersistence, transactions. */
  private fun loginHandler(authenticationResolver: UserAuthenticationResolver): UserHandler =
    UserHandler(
      userPersistence,
      mockk<ExternalUserService>(relaxed = true),
      mockk<OrganizationService>(relaxed = true),
      mockk<SsoConfigService>(relaxed = true),
      mockk<OrganizationEmailDomainService>(relaxed = true),
      Optional.empty(),
      mockk<PermissionHandler>(relaxed = true),
      mockk<WorkspacesHandler>(relaxed = true),
      Supplier { UUID.randomUUID() },
      authenticationResolver,
      Optional.empty<InitialUserConfig>(),
      mockk<ResourceBootstrapHandlerInterface>(relaxed = true),
      mockk<FeatureFlagClient>(relaxed = true),
      firstLoginService,
      transactions,
    )

  private fun input(
    userName: String,
    externalId: String,
    active: Boolean = true,
  ): ScimUserWrite =
    ScimUserWrite(
      userName = userName,
      externalId = externalId,
      primaryEmail = userName,
      active = active,
      attributes =
        objectMapper.createObjectNode().also {
          it.put("displayName", "Gate Test User")
          it
            .putArray("emails")
            .addObject()
            .put("value", userName)
            .put("type", "work")
            .put("primary", true)
        },
    )

  companion object {
    private const val VICTIM_EMAIL = "victim@victim.com"
    private const val ATTACKER_EMAIL = "attacker@evil.example"

    private lateinit var context: ApplicationContext
    private lateinit var jooq: DSLContext
    private lateinit var objectMapper: ObjectMapper
    private lateinit var organizationRepository: OrganizationRepository
    private lateinit var configurationRepository: ScimConfigurationRepository
    private lateinit var mappingRepository: ScimResourceMappingRepository
    private lateinit var userRepository: ScimAirbyteUserRepository
    private lateinit var permissionRepository: PermissionRepository
    private lateinit var domainVerificationRepository: OrganizationDomainVerificationRepository
    private lateinit var permissionService: PermissionService
    private lateinit var lifecycleService: ScimUserLifecycleService
    private lateinit var firstLoginService: ScimFirstLoginService
    private lateinit var mutationService: ScimMutationService
    private lateinit var userPersistence: UserPersistence
    private lateinit var dataSource: DataSource
    private lateinit var database: Database
    private lateinit var transactions: TransactionOperations<Connection>

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
            "scim-verified-email-domain-gate-test",
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

      objectMapper = context.getBean(ObjectMapper::class.java)
      organizationRepository = context.getBean(OrganizationRepository::class.java)
      configurationRepository = context.getBean(ScimConfigurationRepository::class.java)
      mappingRepository = context.getBean(ScimResourceMappingRepository::class.java)
      userRepository = context.getBean(ScimAirbyteUserRepository::class.java)
      permissionRepository = context.getBean(PermissionRepository::class.java)
      domainVerificationRepository = context.getBean(OrganizationDomainVerificationRepository::class.java)
      permissionService = context.getBean(PermissionService::class.java)
      firstLoginService = context.getBean(ScimFirstLoginService::class.java)

      @Suppress("UNCHECKED_CAST")
      transactions =
        context.getBean(TransactionOperations::class.java, Qualifiers.byName("config")) as TransactionOperations<Connection>
      lifecycleService =
        ScimUserLifecycleService(
          mappingRepository,
          userRepository,
          permissionRepository,
          context.getBean(GroupMemberRepository::class.java),
          domainVerificationRepository,
        )
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
