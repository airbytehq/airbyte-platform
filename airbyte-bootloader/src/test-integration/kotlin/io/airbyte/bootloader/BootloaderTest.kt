/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.resources.MoreResources
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.commons.version.Version
import io.airbyte.config.Configs
import io.airbyte.config.Configs.SeedDefinitionsProviderType
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.init.ApplyDefinitionsHelper
import io.airbyte.config.init.BreakingChangeNotificationHelper
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult
import io.airbyte.config.init.DeclarativeManifestImageVersionsProvider
import io.airbyte.config.init.DeclarativeSourceUpdater
import io.airbyte.config.init.LocalDeclarativeManifestImageVersionsProvider
import io.airbyte.config.init.PostLoadExecutor
import io.airbyte.config.init.SupportStateUpdater
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.config.persistence.BreakingChangesHelper
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.specs.DefinitionsProvider
import io.airbyte.config.specs.LocalDefinitionsProvider
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.factory.DataSourceFactory
import io.airbyte.db.factory.DatabaseCheckFactory
import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.ConfigsDatabaseTestProvider
import io.airbyte.db.instance.configs.migrations.V1_1_1_010__CreateWorkloadQueueTable
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator
import io.airbyte.db.instance.jobs.JobsDatabaseTestProvider
import io.airbyte.db.instance.jobs.migrations.V1_1_0_001__AddIsScheduledToJobTable
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.DefaultJobPersistence
import org.flywaydb.core.Flyway
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.testcontainers.containers.PostgreSQLContainer
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables
import uk.org.webcompere.systemstubs.jupiter.SystemStub
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

/**
 * Test suite for the [Bootloader] class.
 */
@ExtendWith(SystemStubsExtension::class)
internal class BootloaderTest {
  private var container: PostgreSQLContainer<*>? = null
  private var configsDataSource: DataSource? = null
  private var jobsDataSource: DataSource? = null
  private var featureFlagClient: FeatureFlagClient? = null
  private var metricClient = Mockito.mock<MetricClient>()

  @BeforeEach
  fun setup() {
    container =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("public")
        .withUsername(DOCKER)
        .withPassword(DOCKER)
    container!!.start()

    configsDataSource =
      DataSourceFactory.create(container!!.username, container!!.password, container!!.driverClassName, container!!.jdbcUrl)
    jobsDataSource =
      DataSourceFactory.create(container!!.username, container!!.password, container!!.driverClassName, container!!.jdbcUrl)

    featureFlagClient = TestClient(mapOf("heartbeat-max-seconds-between-messages" to "10800"))
  }

  @AfterEach
  fun cleanup() {
    closeDataSource(configsDataSource)
    closeDataSource(jobsDataSource)
    container!!.stop()
  }

  @SystemStub
  private val environmentVariables: EnvironmentVariables? = null

  @Test
  fun testBootloaderAppBlankDb() {
    val currentAirbyteVersion = AirbyteVersion(VERSION_0330_ALPHA)
    // The protocol version range should contain our default protocol version since many definitions we
    // load don't provide a protocol version.
    val airbyteProtocolRange =
      AirbyteProtocolVersionRange(
        Version(PROTOCOL_VERSION_001),
        Version(
          PROTOCOL_VERSION_124,
        ),
      )
    val runMigrationOnStartup = true

    val configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES)
    val jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES)

    val configsFlyway = createConfigsFlyway(configsDataSource)
    val jobsFlyway = createJobsFlyway(jobsDataSource)

    val configDatabase = ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false)
    val jobDatabase = JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false)
    val secretsRepositoryReader = Mockito.mock(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = Mockito.mock(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService =
      Mockito.mock(
        SecretPersistenceConfigService::class.java,
      )
    val connectionService = ConnectionServiceJooqImpl(configDatabase)
    val actorDefinitionService = ActorDefinitionServiceJooqImpl(configDatabase)
    val scopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    val connectionTimelineService =
      Mockito.mock(
        ConnectionTimelineEventService::class.java,
      )
    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient!!,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineService,
      )
    val destinationService =
      DestinationServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
      )
    val sourceService =
      SourceServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
      )
    val workspaceService =
      WorkspaceServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )
    val configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L)
    val configDatabaseInitializer =
      DatabaseCheckFactory.createConfigsDatabaseInitializer(
        configsDslContext,
        configsDatabaseInitializationTimeoutMs,
        MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH),
      )
    val configsDatabaseMigrator = ConfigsDatabaseMigrator(configDatabase, configsFlyway)
    val definitionsProvider: DefinitionsProvider = LocalDefinitionsProvider()
    val jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L)
    val jobsDatabaseInitializer =
      DatabaseCheckFactory.createJobsDatabaseInitializer(
        jobsDslContext,
        jobsDatabaseInitializationTimeoutMs,
        MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH),
      )
    val jobsDatabaseMigrator = JobsDatabaseMigrator(jobDatabase, jobsFlyway)
    val jobsPersistence = DefaultJobPersistence(jobDatabase)
    val organizationPersistence = OrganizationPersistence(jobDatabase)
    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobsPersistence,
        airbyteProtocolRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val breakingChangeNotificationHelper =
      BreakingChangeNotificationHelper(
        workspaceService,
        featureFlagClient!!,
      )
    val breakingChangeHelper = BreakingChangesHelper(scopedConfigurationService, workspaceService, destinationService, sourceService)
    val supportStateUpdater =
      SupportStateUpdater(
        actorDefinitionService,
        sourceService,
        destinationService,
        Configs.AirbyteEdition.COMMUNITY,
        breakingChangeHelper,
        breakingChangeNotificationHelper,
        featureFlagClient!!,
      )
    val actorDefinitionVersionResolver =
      Mockito.mock(
        ActorDefinitionVersionResolver::class.java,
      )
    val airbyteCompatibleConnectorsValidator =
      Mockito.mock(
        AirbyteCompatibleConnectorsValidator::class.java,
      )
    val connectorRolloutService = Mockito.mock(ConnectorRolloutService::class.java)
    Mockito
      .`when`(airbyteCompatibleConnectorsValidator.validate(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
      .thenReturn(ConnectorPlatformCompatibilityValidationResult(true, ""))
    Mockito
      .`when`(airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(ArgumentMatchers.anyString()))
      .thenReturn(ConnectorPlatformCompatibilityValidationResult(true, ""))
    val applyDefinitionsHelper =
      ApplyDefinitionsHelper(
        definitionsProvider,
        SEED_PROVIDER_TYPE,
        jobsPersistence,
        actorDefinitionService,
        sourceService,
        destinationService,
        metricClient,
        supportStateUpdater,
        actorDefinitionVersionResolver,
        airbyteCompatibleConnectorsValidator,
        connectorRolloutService,
      )
    val declarativeManifestImageVersionsProvider: DeclarativeManifestImageVersionsProvider = LocalDeclarativeManifestImageVersionsProvider()
    val declarativeSourceUpdater =
      DeclarativeSourceUpdater(
        declarativeManifestImageVersionsProvider,
        Mockito.mock(
          DeclarativeManifestImageVersionService::class.java,
        ),
        actorDefinitionService,
        airbyteCompatibleConnectorsValidator,
        featureFlagClient!!,
      )
    val authKubeSecretInitializer =
      Mockito.mock(
        AuthKubernetesSecretInitializer::class.java,
      )
    val postLoadExecutor =
      DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, authKubeSecretInitializer)

    val bootloader =
      Bootloader(
        false,
        workspaceService,
        configDatabaseInitializer,
        configsDatabaseMigrator,
        currentAirbyteVersion,
        jobsDatabaseInitializer,
        jobsDatabaseMigrator,
        jobsPersistence,
        organizationPersistence,
        protocolVersionChecker,
        runMigrationOnStartup,
        DEFAULT_REALM,
        postLoadExecutor,
      )
    bootloader.load()

    val jobsMigrator = JobsDatabaseMigrator(jobDatabase, jobsFlyway)
    Assertions.assertEquals(getMigrationVersion(CURRENT_JOBS_MIGRATION), jobsMigrator.latestMigration.version.version)

    val configsMigrator = ConfigsDatabaseMigrator(configDatabase, configsFlyway)
    Assertions.assertEquals(getMigrationVersion(CURRENT_CONFIGS_MIGRATION), configsMigrator.latestMigration.version.version)

    Assertions.assertEquals(VERSION_0330_ALPHA, jobsPersistence.version.get())
    Assertions.assertEquals(Version(PROTOCOL_VERSION_001), jobsPersistence.airbyteProtocolVersionMin.get())
    Assertions.assertEquals(Version(PROTOCOL_VERSION_124), jobsPersistence.airbyteProtocolVersionMax.get())

    Assertions.assertNotEquals(Optional.empty<Any>(), jobsPersistence.deployment)

    Assertions.assertEquals(
      DEFAULT_REALM,
      organizationPersistence.getSsoConfigForOrganization(OrganizationPersistence.DEFAULT_ORGANIZATION_ID).get().keycloakRealm,
    )
  }

  @Test
  fun testRequiredVersionUpgradePredicate() {
    val currentAirbyteVersion = AirbyteVersion(VERSION_0330_ALPHA)
    val airbyteProtocolRange =
      AirbyteProtocolVersionRange(
        Version(PROTOCOL_VERSION_001),
        Version(
          PROTOCOL_VERSION_124,
        ),
      )
    val runMigrationOnStartup = true

    val configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES)
    val jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES)

    val configsFlyway = createConfigsFlyway(configsDataSource)
    val jobsFlyway = createJobsFlyway(jobsDataSource)

    val configDatabase = ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false)
    val jobDatabase = JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false)
    val connectionService = ConnectionServiceJooqImpl(configDatabase)
    val actorDefinitionService = ActorDefinitionServiceJooqImpl(configDatabase)
    val scopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    val connectionTimelineService =
      Mockito.mock(
        ConnectionTimelineEventService::class.java,
      )
    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient!!,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineService,
      )
    val sourceService =
      SourceServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        Mockito.mock(SecretsRepositoryReader::class.java),
        Mockito.mock(SecretsRepositoryWriter::class.java),
        Mockito.mock(SecretPersistenceConfigService::class.java),
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
      )
    val destinationService =
      DestinationServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        Mockito.mock(SecretsRepositoryReader::class.java),
        Mockito.mock(SecretsRepositoryWriter::class.java),
        Mockito.mock(SecretPersistenceConfigService::class.java),
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
      )
    val workspaceService =
      WorkspaceServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        Mockito.mock(SecretsRepositoryReader::class.java),
        Mockito.mock(SecretsRepositoryWriter::class.java),
        Mockito.mock(SecretPersistenceConfigService::class.java),
        metricClient,
      )
    val configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L)
    val configDatabaseInitializer =
      DatabaseCheckFactory.createConfigsDatabaseInitializer(
        configsDslContext,
        configsDatabaseInitializationTimeoutMs,
        MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH),
      )
    val configsDatabaseMigrator = ConfigsDatabaseMigrator(configDatabase, configsFlyway)
    val definitionsProvider: DefinitionsProvider = LocalDefinitionsProvider()
    val jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L)
    val jobsDatabaseInitializer =
      DatabaseCheckFactory.createJobsDatabaseInitializer(
        jobsDslContext,
        jobsDatabaseInitializationTimeoutMs,
        MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH),
      )
    val jobsDatabaseMigrator = JobsDatabaseMigrator(jobDatabase, jobsFlyway)
    val jobsPersistence = DefaultJobPersistence(jobDatabase)
    val organizationPersistence = OrganizationPersistence(jobDatabase)
    val breakingChangeNotificationHelper =
      BreakingChangeNotificationHelper(
        workspaceService,
        featureFlagClient!!,
      )
    val breakingChangesHelper = BreakingChangesHelper(scopedConfigurationService, workspaceService, destinationService, sourceService)
    val supportStateUpdater =
      SupportStateUpdater(
        actorDefinitionService,
        sourceService,
        destinationService,
        Configs.AirbyteEdition.COMMUNITY,
        breakingChangesHelper,
        breakingChangeNotificationHelper,
        featureFlagClient!!,
      )
    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobsPersistence,
        airbyteProtocolRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val actorDefinitionVersionResolver =
      Mockito.mock(
        ActorDefinitionVersionResolver::class.java,
      )
    val airbyteCompatibleConnectorsValidator =
      Mockito.mock(
        AirbyteCompatibleConnectorsValidator::class.java,
      )
    val connectorRolloutService = Mockito.mock(ConnectorRolloutService::class.java)
    val applyDefinitionsHelper =
      ApplyDefinitionsHelper(
        definitionsProvider,
        SEED_PROVIDER_TYPE,
        jobsPersistence,
        actorDefinitionService,
        sourceService,
        destinationService,
        metricClient,
        supportStateUpdater,
        actorDefinitionVersionResolver,
        airbyteCompatibleConnectorsValidator,
        connectorRolloutService,
      )
    val declarativeManifestImageVersionsProvider: DeclarativeManifestImageVersionsProvider = LocalDeclarativeManifestImageVersionsProvider()
    val declarativeSourceUpdater =
      DeclarativeSourceUpdater(
        declarativeManifestImageVersionsProvider,
        Mockito.mock(
          DeclarativeManifestImageVersionService::class.java,
        ),
        actorDefinitionService,
        airbyteCompatibleConnectorsValidator,
        featureFlagClient!!,
      )
    val authKubeSecretInitializer =
      Mockito.mock(
        AuthKubernetesSecretInitializer::class.java,
      )
    val postLoadExecutor = DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, authKubeSecretInitializer)

    val bootloader =
      Bootloader(
        false,
        workspaceService,
        configDatabaseInitializer,
        configsDatabaseMigrator,
        currentAirbyteVersion,
        jobsDatabaseInitializer,
        jobsDatabaseMigrator,
        jobsPersistence,
        organizationPersistence,
        protocolVersionChecker,
        runMigrationOnStartup,
        DEFAULT_REALM,
        postLoadExecutor,
      )

    // starting from no previous version is always legal.
    Assertions.assertNull(bootloader.getRequiredVersionUpgrade(null, AirbyteVersion("0.17.1-alpha")))
    Assertions.assertNull(bootloader.getRequiredVersionUpgrade(null, AirbyteVersion(VERSION_0320_ALPHA)))
    Assertions.assertNull(bootloader.getRequiredVersionUpgrade(null, AirbyteVersion(VERSION_0321_ALPHA)))
    Assertions.assertNull(bootloader.getRequiredVersionUpgrade(null, AirbyteVersion("0.33.1-alpha")))
    // starting from a version that is pre-breaking migration requires an upgrade to the breaking
    // migration.
    Assertions.assertNull(bootloader.getRequiredVersionUpgrade(AirbyteVersion(VERSION_0170_ALPHA), AirbyteVersion("0.17.1-alpha")))
    Assertions.assertNull(bootloader.getRequiredVersionUpgrade(AirbyteVersion(VERSION_0170_ALPHA), AirbyteVersion("0.18.0-alpha")))
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0170_ALPHA),
        AirbyteVersion(
          VERSION_0320_ALPHA,
        ),
      ),
    )
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0320_ALPHA),
        AirbyteVersion(
          VERSION_0370_ALPHA,
        ),
      ),
    )
    Assertions.assertEquals(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0170_ALPHA),
        AirbyteVersion(
          VERSION_0321_ALPHA,
        ),
      ),
      AirbyteVersion(VERSION_0320_ALPHA),
    )
    Assertions.assertEquals(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0170_ALPHA),
        AirbyteVersion(
          VERSION_0330_ALPHA,
        ),
      ),
      AirbyteVersion(VERSION_0320_ALPHA),
    )
    // going through multiple breaking migrations requires an upgrade to the first breaking migration.
    Assertions.assertEquals(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0170_ALPHA),
        AirbyteVersion(
          VERSION_0370_ALPHA,
        ),
      ),
      AirbyteVersion(VERSION_0320_ALPHA),
    )
    Assertions.assertEquals(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0170_ALPHA),
        AirbyteVersion(
          VERSION_0371_ALPHA,
        ),
      ),
      AirbyteVersion(VERSION_0320_ALPHA),
    )
    Assertions.assertEquals(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0170_ALPHA),
        AirbyteVersion(
          VERSION_0380_ALPHA,
        ),
      ),
      AirbyteVersion(VERSION_0320_ALPHA),
    )
    Assertions.assertEquals(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0320_ALPHA),
        AirbyteVersion(
          VERSION_0371_ALPHA,
        ),
      ),
      AirbyteVersion(VERSION_0370_ALPHA),
    )
    Assertions.assertEquals(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0321_ALPHA),
        AirbyteVersion(
          VERSION_0371_ALPHA,
        ),
      ),
      AirbyteVersion(VERSION_0370_ALPHA),
    )
    // any migration starting at the breaking migration, or after it can upgrade to anything.
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0320_ALPHA),
        AirbyteVersion(
          VERSION_0321_ALPHA,
        ),
      ),
    )
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0320_ALPHA),
        AirbyteVersion(
          VERSION_0330_ALPHA,
        ),
      ),
    )
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0321_ALPHA),
        AirbyteVersion(
          VERSION_0321_ALPHA,
        ),
      ),
    )
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0321_ALPHA),
        AirbyteVersion(
          VERSION_0330_ALPHA,
        ),
      ),
    )
    Assertions.assertNull(bootloader.getRequiredVersionUpgrade(AirbyteVersion(VERSION_0330_ALPHA), AirbyteVersion("0.33.1-alpha")))
    Assertions.assertNull(bootloader.getRequiredVersionUpgrade(AirbyteVersion(VERSION_0330_ALPHA), AirbyteVersion("0.34.0-alpha")))
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0370_ALPHA),
        AirbyteVersion(
          VERSION_0371_ALPHA,
        ),
      ),
    )
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0370_ALPHA),
        AirbyteVersion(
          VERSION_0380_ALPHA,
        ),
      ),
    )
    Assertions.assertNull(
      bootloader.getRequiredVersionUpgrade(
        AirbyteVersion(VERSION_0371_ALPHA),
        AirbyteVersion(
          VERSION_0380_ALPHA,
        ),
      ),
    )
  }

  @Test
  fun testPostLoadExecutionExecutes() {
    val testTriggered = AtomicBoolean()
    val currentAirbyteVersion = AirbyteVersion(VERSION_0330_ALPHA)
    val airbyteProtocolRange =
      AirbyteProtocolVersionRange(
        Version(PROTOCOL_VERSION_001),
        Version(
          PROTOCOL_VERSION_124,
        ),
      )
    val runMigrationOnStartup = true

    val configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES)
    val jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES)

    val configsFlyway = createConfigsFlyway(configsDataSource)
    val jobsFlyway = createJobsFlyway(jobsDataSource)

    val configDatabase = ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false)
    val jobDatabase = JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false)
    val connectionService = ConnectionServiceJooqImpl(configDatabase)
    val actorDefinitionService = ActorDefinitionServiceJooqImpl(configDatabase)
    val scopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    val connectionTimelineService =
      Mockito.mock(
        ConnectionTimelineEventService::class.java,
      )
    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient!!,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineService,
      )

    val secretsRepositoryReader = Mockito.mock(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = Mockito.mock(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService =
      Mockito.mock(
        SecretPersistenceConfigService::class.java,
      )
    val workspaceService =
      WorkspaceServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )
    val sourceService =
      SourceServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        Mockito.mock(SecretsRepositoryReader::class.java),
        Mockito.mock(SecretsRepositoryWriter::class.java),
        Mockito.mock(SecretPersistenceConfigService::class.java),
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
      )
    val destinationService =
      DestinationServiceJooqImpl(
        configDatabase,
        featureFlagClient,
        Mockito.mock(SecretsRepositoryReader::class.java),
        Mockito.mock(SecretsRepositoryWriter::class.java),
        Mockito.mock(SecretPersistenceConfigService::class.java),
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
      )
    val configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L)
    val configDatabaseInitializer =
      DatabaseCheckFactory.createConfigsDatabaseInitializer(
        configsDslContext,
        configsDatabaseInitializationTimeoutMs,
        MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH),
      )
    val configsDatabaseMigrator = ConfigsDatabaseMigrator(configDatabase, configsFlyway)
    val definitionsProvider: DefinitionsProvider = LocalDefinitionsProvider()
    val jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L)
    val jobsDatabaseInitializer =
      DatabaseCheckFactory.createJobsDatabaseInitializer(
        jobsDslContext,
        jobsDatabaseInitializationTimeoutMs,
        MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH),
      )
    val jobsDatabaseMigrator = JobsDatabaseMigrator(jobDatabase, jobsFlyway)
    val jobsPersistence = DefaultJobPersistence(jobDatabase)
    val organizationPersistence = OrganizationPersistence(jobDatabase)
    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobsPersistence,
        airbyteProtocolRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val postLoadExecutor: PostLoadExecutor =
      object : PostLoadExecutor {
        override fun execute() {
          testTriggered.set(true)
        }
      }
    val bootloader =
      Bootloader(
        false,
        workspaceService,
        configDatabaseInitializer,
        configsDatabaseMigrator,
        currentAirbyteVersion,
        jobsDatabaseInitializer,
        jobsDatabaseMigrator,
        jobsPersistence,
        organizationPersistence,
        protocolVersionChecker,
        runMigrationOnStartup,
        DEFAULT_REALM,
        postLoadExecutor,
      )
    bootloader.load()
    Assertions.assertTrue(testTriggered.get())
    Assertions.assertEquals(
      DEFAULT_REALM,
      organizationPersistence.getSsoConfigForOrganization(OrganizationPersistence.DEFAULT_ORGANIZATION_ID).get().keycloakRealm,
    )
  }

  private fun createConfigsFlyway(dataSource: DataSource?): Flyway =
    FlywayFactory.create(
      dataSource,
      javaClass.name,
      ConfigsDatabaseMigrator.DB_IDENTIFIER,
      ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
    )

  private fun createJobsFlyway(dataSource: DataSource?): Flyway =
    FlywayFactory.create(
      dataSource,
      javaClass.name,
      JobsDatabaseMigrator.DB_IDENTIFIER,
      JobsDatabaseMigrator.MIGRATION_FILE_LOCATION,
    )

  private fun closeDataSource(dataSource: DataSource?) {
    DataSourceFactory.close(dataSource)
  }

  companion object {
    private const val DEFAULT_REALM = "airbyte"
    private const val DOCKER = "docker"
    private val SEED_PROVIDER_TYPE = SeedDefinitionsProviderType.LOCAL
    private const val PROTOCOL_VERSION_001 = "0.0.1"
    private const val PROTOCOL_VERSION_124 = "1.2.4"
    private const val VERSION_0330_ALPHA = "0.33.0-alpha"
    private const val VERSION_0320_ALPHA = "0.32.0-alpha"
    private const val VERSION_0321_ALPHA = "0.32.1-alpha"
    private const val VERSION_0370_ALPHA = "0.37.0-alpha"
    private const val VERSION_0371_ALPHA = "0.37.1-alpha"
    private const val VERSION_0380_ALPHA = "0.38.0-alpha"
    private const val VERSION_0170_ALPHA = "0.17.0-alpha"

    // ⚠️ This line should change with every new migration to show that you meant to make a new
    // migration to the prod database
    private val CURRENT_CONFIGS_MIGRATION = V1_1_1_010__CreateWorkloadQueueTable::class.java
    private val CURRENT_JOBS_MIGRATION = V1_1_0_001__AddIsScheduledToJobTable::class.java

    private fun getMigrationVersion(cls: Class<*>): String =
      cls.simpleName
        .split("__")[0]
        .substring(1)
        .replace('_', '.')
  }
}
