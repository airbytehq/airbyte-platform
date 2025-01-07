/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.Configs.SeedDefinitionsProviderType;
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator;
import io.airbyte.config.init.ApplyDefinitionsHelper;
import io.airbyte.config.init.BreakingChangeNotificationHelper;
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult;
import io.airbyte.config.init.DeclarativeManifestImageVersionsProvider;
import io.airbyte.config.init.DeclarativeSourceUpdater;
import io.airbyte.config.init.LocalDeclarativeManifestImageVersionsProvider;
import io.airbyte.config.init.PostLoadExecutor;
import io.airbyte.config.init.SupportStateUpdater;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.config.persistence.BreakingChangesHelper;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.specs.DefinitionsProvider;
import io.airbyte.config.specs.LocalDefinitionsProvider;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.ConnectorRolloutService;
import io.airbyte.data.services.DeclarativeManifestImageVersionService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.factory.DatabaseCheckFactory;
import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.ConfigsDatabaseTestProvider;
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator;
import io.airbyte.db.instance.jobs.JobsDatabaseTestProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.lib.NotImplementedMetricClient;
import io.airbyte.persistence.job.DefaultJobPersistence;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

/**
 * Test suite for the {@link Bootloader} class.
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
@ExtendWith(SystemStubsExtension.class)
class BootloaderTest {

  private PostgreSQLContainer container;
  private DataSource configsDataSource;
  private DataSource jobsDataSource;
  private FeatureFlagClient featureFlagClient;
  private static final String DEFAULT_REALM = "airbyte";
  private static final String DOCKER = "docker";
  private static final SeedDefinitionsProviderType SEED_PROVIDER_TYPE = SeedDefinitionsProviderType.LOCAL;
  private static final String PROTOCOL_VERSION_001 = "0.0.1";
  private static final String PROTOCOL_VERSION_124 = "1.2.4";
  private static final String VERSION_0330_ALPHA = "0.33.0-alpha";
  private static final String VERSION_0320_ALPHA = "0.32.0-alpha";
  private static final String VERSION_0321_ALPHA = "0.32.1-alpha";
  private static final String VERSION_0370_ALPHA = "0.37.0-alpha";
  private static final String VERSION_0371_ALPHA = "0.37.1-alpha";
  private static final String VERSION_0380_ALPHA = "0.38.0-alpha";
  private static final String VERSION_0170_ALPHA = "0.17.0-alpha";

  // ⚠️ This line should change with every new migration to show that you meant to make a new
  // migration to the prod database
  private static final String CURRENT_CONFIGS_MIGRATION_VERSION = "1.1.0.010";
  private static final String CURRENT_JOBS_MIGRATION_VERSION = "1.1.0.000";

  @BeforeEach
  void setup() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("public")
        .withUsername(DOCKER)
        .withPassword(DOCKER);
    container.start();

    configsDataSource =
        DataSourceFactory.create(container.getUsername(), container.getPassword(), container.getDriverClassName(), container.getJdbcUrl());
    jobsDataSource =
        DataSourceFactory.create(container.getUsername(), container.getPassword(), container.getDriverClassName(), container.getJdbcUrl());

    featureFlagClient = new TestClient(Map.of("heartbeat-max-seconds-between-messages", "10800"));
  }

  @AfterEach
  void cleanup() throws Exception {
    closeDataSource(configsDataSource);
    closeDataSource(jobsDataSource);
    container.stop();
  }

  @SystemStub
  private EnvironmentVariables environmentVariables;

  @Test
  void testBootloaderAppBlankDb() throws Exception {
    var currentAirbyteVersion = new AirbyteVersion(VERSION_0330_ALPHA);
    // The protocol version range should contain our default protocol version since many definitions we
    // load don't provide a protocol version.
    var airbyteProtocolRange = new AirbyteProtocolVersionRange(new Version(PROTOCOL_VERSION_001), new Version(PROTOCOL_VERSION_124));
    var runMigrationOnStartup = true;

    var configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);
    var jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);

    var configsFlyway = createConfigsFlyway(configsDataSource);
    var jobsFlyway = createJobsFlyway(jobsDataSource);

    var configDatabase = new ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false);
    var jobDatabase = new JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    var connectionService = new ConnectionServiceJooqImpl(configDatabase);
    var actorDefinitionService = new ActorDefinitionServiceJooqImpl(configDatabase);
    var scopedConfigurationService = mock(ScopedConfigurationService.class);
    var connectionTimelineService = mock(ConnectionTimelineEventService.class);
    var actorDefinitionVersionUpdater = new ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineService);
    var destinationService = new DestinationServiceJooqImpl(configDatabase,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater);
    var sourceService = new SourceServiceJooqImpl(configDatabase,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater);
    var workspaceService = new WorkspaceServiceJooqImpl(configDatabase,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService);
    var configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    var configDatabaseInitializer = DatabaseCheckFactory.createConfigsDatabaseInitializer(configsDslContext,
        configsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH));
    var configsDatabaseMigrator = new ConfigsDatabaseMigrator(configDatabase, configsFlyway);
    final DefinitionsProvider definitionsProvider = new LocalDefinitionsProvider();
    var jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    var jobsDatabaseInitializer = DatabaseCheckFactory.createJobsDatabaseInitializer(jobsDslContext,
        jobsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH));
    var jobsDatabaseMigrator = new JobsDatabaseMigrator(jobDatabase, jobsFlyway);
    var jobsPersistence = new DefaultJobPersistence(jobDatabase);
    var organizationPersistence = new OrganizationPersistence(jobDatabase);
    var protocolVersionChecker =
        new ProtocolVersionChecker(jobsPersistence, airbyteProtocolRange, actorDefinitionService, definitionsProvider, sourceService,
            destinationService);
    var breakingChangeNotificationHelper = new BreakingChangeNotificationHelper(workspaceService, featureFlagClient);
    var breakingChangeHelper = new BreakingChangesHelper(scopedConfigurationService, workspaceService, destinationService, sourceService);
    var supportStateUpdater =
        new SupportStateUpdater(actorDefinitionService, sourceService, destinationService, DeploymentMode.OSS, breakingChangeHelper,
            breakingChangeNotificationHelper, featureFlagClient);
    var metricClient = new NotImplementedMetricClient();
    var actorDefinitionVersionResolver = mock(ActorDefinitionVersionResolver.class);
    var airbyteCompatibleConnectorsValidator = mock(AirbyteCompatibleConnectorsValidator.class);
    var connectorRolloutService = mock(ConnectorRolloutService.class);
    when(airbyteCompatibleConnectorsValidator.validate(anyString(), anyString()))
        .thenReturn(new ConnectorPlatformCompatibilityValidationResult(true, ""));
    when(airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(anyString()))
        .thenReturn(new ConnectorPlatformCompatibilityValidationResult(true, ""));
    var applyDefinitionsHelper =
        new ApplyDefinitionsHelper(definitionsProvider, SEED_PROVIDER_TYPE, jobsPersistence, actorDefinitionService, sourceService,
            destinationService,
            metricClient, supportStateUpdater, actorDefinitionVersionResolver, airbyteCompatibleConnectorsValidator, connectorRolloutService);
    final DeclarativeManifestImageVersionsProvider declarativeManifestImageVersionsProvider = new LocalDeclarativeManifestImageVersionsProvider();
    var declarativeSourceUpdater =
        new DeclarativeSourceUpdater(declarativeManifestImageVersionsProvider, mock(DeclarativeManifestImageVersionService.class),
            actorDefinitionService, airbyteCompatibleConnectorsValidator, featureFlagClient);
    var authKubeSecretInitializer = mock(AuthKubernetesSecretInitializer.class);
    var postLoadExecutor =
        new DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, Optional.of(authKubeSecretInitializer));

    var bootloader =
        new Bootloader(false, workspaceService, configDatabaseInitializer, configsDatabaseMigrator, currentAirbyteVersion, jobsDatabaseInitializer,
            jobsDatabaseMigrator, jobsPersistence, organizationPersistence, protocolVersionChecker,
            runMigrationOnStartup, DEFAULT_REALM, postLoadExecutor);
    bootloader.load();

    var jobsMigrator = new JobsDatabaseMigrator(jobDatabase, jobsFlyway);
    assertEquals(CURRENT_JOBS_MIGRATION_VERSION, jobsMigrator.getLatestMigration().getVersion().getVersion());

    var configsMigrator = new ConfigsDatabaseMigrator(configDatabase, configsFlyway);
    assertEquals(CURRENT_CONFIGS_MIGRATION_VERSION, configsMigrator.getLatestMigration().getVersion().getVersion());

    assertEquals(VERSION_0330_ALPHA, jobsPersistence.getVersion().get());
    assertEquals(new Version(PROTOCOL_VERSION_001), jobsPersistence.getAirbyteProtocolVersionMin().get());
    assertEquals(new Version(PROTOCOL_VERSION_124), jobsPersistence.getAirbyteProtocolVersionMax().get());

    assertNotEquals(Optional.empty(), jobsPersistence.getDeployment());

    assertEquals(DEFAULT_REALM,
        organizationPersistence.getSsoConfigForOrganization(OrganizationPersistence.DEFAULT_ORGANIZATION_ID).get().getKeycloakRealm());
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  void testRequiredVersionUpgradePredicate() throws Exception {
    var currentAirbyteVersion = new AirbyteVersion(VERSION_0330_ALPHA);
    var airbyteProtocolRange = new AirbyteProtocolVersionRange(new Version(PROTOCOL_VERSION_001), new Version(PROTOCOL_VERSION_124));
    var runMigrationOnStartup = true;

    var configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);
    var jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);

    var configsFlyway = createConfigsFlyway(configsDataSource);
    var jobsFlyway = createJobsFlyway(jobsDataSource);

    var configDatabase = new ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false);
    var jobDatabase = new JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false);
    var connectionService = new ConnectionServiceJooqImpl(configDatabase);
    var actorDefinitionService = new ActorDefinitionServiceJooqImpl(configDatabase);
    var scopedConfigurationService = mock(ScopedConfigurationService.class);
    var connectionTimelineService = mock(ConnectionTimelineEventService.class);
    var actorDefinitionVersionUpdater = new ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineService);
    var sourceService = new SourceServiceJooqImpl(configDatabase,
        featureFlagClient,
        mock(SecretsRepositoryReader.class),
        mock(SecretsRepositoryWriter.class),
        mock(SecretPersistenceConfigService.class),
        connectionService,
        actorDefinitionVersionUpdater);
    var destinationService = new DestinationServiceJooqImpl(configDatabase,
        featureFlagClient,
        mock(SecretsRepositoryReader.class),
        mock(SecretsRepositoryWriter.class),
        mock(SecretPersistenceConfigService.class),
        connectionService,
        actorDefinitionVersionUpdater);
    var workspaceService = new WorkspaceServiceJooqImpl(configDatabase,
        featureFlagClient,
        mock(SecretsRepositoryReader.class),
        mock(SecretsRepositoryWriter.class),
        mock(SecretPersistenceConfigService.class));
    var configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    var configDatabaseInitializer = DatabaseCheckFactory.createConfigsDatabaseInitializer(configsDslContext,
        configsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH));
    var configsDatabaseMigrator = new ConfigsDatabaseMigrator(configDatabase, configsFlyway);
    final DefinitionsProvider definitionsProvider = new LocalDefinitionsProvider();
    var jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    var jobsDatabaseInitializer = DatabaseCheckFactory.createJobsDatabaseInitializer(jobsDslContext,
        jobsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH));
    var jobsDatabaseMigrator = new JobsDatabaseMigrator(jobDatabase, jobsFlyway);
    var jobsPersistence = new DefaultJobPersistence(jobDatabase);
    var organizationPersistence = new OrganizationPersistence(jobDatabase);
    var breakingChangeNotificationHelper = new BreakingChangeNotificationHelper(workspaceService, featureFlagClient);
    var breakingChangesHelper = new BreakingChangesHelper(scopedConfigurationService, workspaceService, destinationService, sourceService);
    var supportStateUpdater =
        new SupportStateUpdater(actorDefinitionService, sourceService, destinationService, DeploymentMode.OSS, breakingChangesHelper,
            breakingChangeNotificationHelper, featureFlagClient);
    var protocolVersionChecker =
        new ProtocolVersionChecker(jobsPersistence, airbyteProtocolRange, actorDefinitionService, definitionsProvider, sourceService,
            destinationService);
    var metricClient = new NotImplementedMetricClient();
    var actorDefinitionVersionResolver = mock(ActorDefinitionVersionResolver.class);
    var airbyteCompatibleConnectorsValidator = mock(AirbyteCompatibleConnectorsValidator.class);
    var connectorRolloutService = mock(ConnectorRolloutService.class);
    var applyDefinitionsHelper =
        new ApplyDefinitionsHelper(definitionsProvider, SEED_PROVIDER_TYPE, jobsPersistence, actorDefinitionService, sourceService,
            destinationService,
            metricClient, supportStateUpdater, actorDefinitionVersionResolver, airbyteCompatibleConnectorsValidator, connectorRolloutService);
    final DeclarativeManifestImageVersionsProvider declarativeManifestImageVersionsProvider = new LocalDeclarativeManifestImageVersionsProvider();
    var declarativeSourceUpdater =
        new DeclarativeSourceUpdater(declarativeManifestImageVersionsProvider, mock(DeclarativeManifestImageVersionService.class),
            actorDefinitionService, airbyteCompatibleConnectorsValidator, featureFlagClient);
    var authKubeSecretInitializer = mock(AuthKubernetesSecretInitializer.class);
    var postLoadExecutor = new DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, Optional.of(authKubeSecretInitializer));

    var bootloader =
        new Bootloader(false, workspaceService, configDatabaseInitializer, configsDatabaseMigrator, currentAirbyteVersion, jobsDatabaseInitializer,
            jobsDatabaseMigrator, jobsPersistence, organizationPersistence, protocolVersionChecker,
            runMigrationOnStartup, DEFAULT_REALM, postLoadExecutor);

    // starting from no previous version is always legal.
    assertEquals(bootloader.getRequiredVersionUpgrade(null, new AirbyteVersion("0.17.1-alpha")), Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(null, new AirbyteVersion(VERSION_0320_ALPHA)), Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(null, new AirbyteVersion(VERSION_0321_ALPHA)), Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(null, new AirbyteVersion("0.33.1-alpha")), Optional.empty());
    // starting from a version that is pre-breaking migration requires an upgrade to the breaking
    // migration.
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0170_ALPHA), new AirbyteVersion("0.17.1-alpha")), Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0170_ALPHA), new AirbyteVersion("0.18.0-alpha")), Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0170_ALPHA), new AirbyteVersion(VERSION_0320_ALPHA)),
        Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0320_ALPHA), new AirbyteVersion(VERSION_0370_ALPHA)),
        Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0170_ALPHA), new AirbyteVersion(VERSION_0321_ALPHA)),
        Optional.of(new AirbyteVersion(VERSION_0320_ALPHA)));
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0170_ALPHA), new AirbyteVersion(VERSION_0330_ALPHA)),
        Optional.of(new AirbyteVersion(VERSION_0320_ALPHA)));
    // going through multiple breaking migrations requires an upgrade to the first breaking migration.
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0170_ALPHA), new AirbyteVersion(VERSION_0370_ALPHA)),
        Optional.of(new AirbyteVersion(VERSION_0320_ALPHA)));
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0170_ALPHA), new AirbyteVersion(VERSION_0371_ALPHA)),
        Optional.of(new AirbyteVersion(VERSION_0320_ALPHA)));
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0170_ALPHA), new AirbyteVersion(VERSION_0380_ALPHA)),
        Optional.of(new AirbyteVersion(VERSION_0320_ALPHA)));
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0320_ALPHA), new AirbyteVersion(VERSION_0371_ALPHA)),
        Optional.of(new AirbyteVersion(VERSION_0370_ALPHA)));
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0321_ALPHA), new AirbyteVersion(VERSION_0371_ALPHA)),
        Optional.of(new AirbyteVersion(VERSION_0370_ALPHA)));
    // any migration starting at the breaking migration or after it can upgrade to anything.
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0320_ALPHA), new AirbyteVersion(VERSION_0321_ALPHA)),
        Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0320_ALPHA), new AirbyteVersion(VERSION_0330_ALPHA)),
        Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0321_ALPHA), new AirbyteVersion(VERSION_0321_ALPHA)),
        Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0321_ALPHA), new AirbyteVersion(VERSION_0330_ALPHA)),
        Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0330_ALPHA), new AirbyteVersion("0.33.1-alpha")), Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0330_ALPHA), new AirbyteVersion("0.34.0-alpha")), Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0370_ALPHA), new AirbyteVersion(VERSION_0371_ALPHA)),
        Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0370_ALPHA), new AirbyteVersion(VERSION_0380_ALPHA)),
        Optional.empty());
    assertEquals(bootloader.getRequiredVersionUpgrade(new AirbyteVersion(VERSION_0371_ALPHA), new AirbyteVersion(VERSION_0380_ALPHA)),
        Optional.empty());
  }

  @Test
  void testPostLoadExecutionExecutes() throws Exception {
    final var testTriggered = new AtomicBoolean();
    var currentAirbyteVersion = new AirbyteVersion(VERSION_0330_ALPHA);
    var airbyteProtocolRange = new AirbyteProtocolVersionRange(new Version(PROTOCOL_VERSION_001), new Version(PROTOCOL_VERSION_124));
    var runMigrationOnStartup = true;

    var configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);
    var jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);

    var configsFlyway = createConfigsFlyway(configsDataSource);
    var jobsFlyway = createJobsFlyway(jobsDataSource);

    var configDatabase = new ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false);
    var jobDatabase = new JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false);
    var connectionService = new ConnectionServiceJooqImpl(configDatabase);
    var actorDefinitionService = new ActorDefinitionServiceJooqImpl(configDatabase);
    var scopedConfigurationService = mock(ScopedConfigurationService.class);
    var connectionTimelineService = mock(ConnectionTimelineEventService.class);
    var actorDefinitionVersionUpdater = new ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineService);

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    var workspaceService = new WorkspaceServiceJooqImpl(configDatabase,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService);
    var sourceService = new SourceServiceJooqImpl(configDatabase,
        featureFlagClient,
        mock(SecretsRepositoryReader.class),
        mock(SecretsRepositoryWriter.class),
        mock(SecretPersistenceConfigService.class),
        connectionService,
        actorDefinitionVersionUpdater);
    var destinationService = new DestinationServiceJooqImpl(configDatabase,
        featureFlagClient,
        mock(SecretsRepositoryReader.class),
        mock(SecretsRepositoryWriter.class),
        mock(SecretPersistenceConfigService.class),
        connectionService,
        actorDefinitionVersionUpdater);
    var configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    var configDatabaseInitializer = DatabaseCheckFactory.createConfigsDatabaseInitializer(configsDslContext,
        configsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH));
    var configsDatabaseMigrator = new ConfigsDatabaseMigrator(configDatabase, configsFlyway);
    final DefinitionsProvider definitionsProvider = new LocalDefinitionsProvider();
    var jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    var jobsDatabaseInitializer = DatabaseCheckFactory.createJobsDatabaseInitializer(jobsDslContext,
        jobsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH));
    var jobsDatabaseMigrator = new JobsDatabaseMigrator(jobDatabase, jobsFlyway);
    var jobsPersistence = new DefaultJobPersistence(jobDatabase);
    var organizationPersistence = new OrganizationPersistence(jobDatabase);
    var protocolVersionChecker =
        new ProtocolVersionChecker(jobsPersistence, airbyteProtocolRange, actorDefinitionService, definitionsProvider, sourceService,
            destinationService);
    var postLoadExecutor = new PostLoadExecutor() {

      @Override
      public void execute() {
        testTriggered.set(true);
      }

    };
    var bootloader =
        new Bootloader(false, workspaceService, configDatabaseInitializer, configsDatabaseMigrator, currentAirbyteVersion,
            jobsDatabaseInitializer, jobsDatabaseMigrator, jobsPersistence, organizationPersistence, protocolVersionChecker,
            runMigrationOnStartup, DEFAULT_REALM, postLoadExecutor);
    bootloader.load();
    assertTrue(testTriggered.get());
    assertEquals(DEFAULT_REALM,
        organizationPersistence.getSsoConfigForOrganization(OrganizationPersistence.DEFAULT_ORGANIZATION_ID).get().getKeycloakRealm());
  }

  private Flyway createConfigsFlyway(final DataSource dataSource) {
    return FlywayFactory.create(dataSource, getClass().getName(), ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
  }

  private Flyway createJobsFlyway(final DataSource dataSource) {
    return FlywayFactory.create(dataSource, getClass().getName(), JobsDatabaseMigrator.DB_IDENTIFIER,
        JobsDatabaseMigrator.MIGRATION_FILE_LOCATION);
  }

  private void closeDataSource(final DataSource dataSource) throws Exception {
    DataSourceFactory.close(dataSource);
  }

}
