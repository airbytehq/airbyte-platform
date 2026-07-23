/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.airbyte.domain.models.scim.ScimConfigurationConflictException
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimIdpProvider
import io.airbyte.domain.services.scim.ScimAccessGate
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.domain.services.scim.ScimTokenService
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.data.exceptions.DataAccessException
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
  ): ScimConfigurationService {
    val gate = mockk<ScimAccessGate>()
    every { gate.isAllowed(organizationId) } returns true
    return ScimConfigurationService(
      gate,
      organizationRepository,
      scimConfigurationRepository,
      tokenService,
      configTransactionOperations,
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

  companion object {
    private lateinit var context: ApplicationContext
    private lateinit var jooqDslContext: DSLContext
    private lateinit var organizationRepository: OrganizationRepository
    private lateinit var scimConfigurationRepository: ScimConfigurationRepository
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
