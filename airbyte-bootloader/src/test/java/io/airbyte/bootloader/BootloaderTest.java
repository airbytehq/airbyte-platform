/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.config.init.ApplyDefinitionsHelper;
import io.airbyte.config.init.CdkVersionProvider;
import io.airbyte.config.init.DeclarativeSourceUpdater;
import io.airbyte.config.init.DefinitionsProvider;
import io.airbyte.config.init.LocalDefinitionsProvider;
import io.airbyte.config.init.PostLoadExecutor;
import io.airbyte.config.persistence.ActorDefinitionMigrator;
import io.airbyte.config.persistence.ConfigRepository;
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
import io.airbyte.persistence.job.DefaultJobPersistence;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import lombok.val;
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
  private static final String DOCKER = "docker";
  private static final String PROTOCOL_VERSION_123 = "1.2.3";
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
  private static final String CURRENT_CONFIGS_MIGRATION_VERSION = "0.50.6.002";
  private static final String CURRENT_JOBS_MIGRATION_VERSION = "0.50.4.001";
  private static final String CDK_VERSION = "1.2.3";

  @BeforeEach
  void setup() {
    container = new PostgreSQLContainer<>("postgres:13-alpine")
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
    val currentAirbyteVersion = new AirbyteVersion(VERSION_0330_ALPHA);
    val airbyteProtocolRange = new AirbyteProtocolVersionRange(new Version(PROTOCOL_VERSION_123), new Version(PROTOCOL_VERSION_124));
    val mockedFeatureFlags = mock(FeatureFlags.class);
    val runMigrationOnStartup = true;

    val configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);
    val jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);

    val configsFlyway = createConfigsFlyway(configsDataSource);
    val jobsFlyway = createJobsFlyway(jobsDataSource);

    val configDatabase = new ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false);
    val jobDatabase = new JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false);
    val configRepository = new ConfigRepository(configDatabase, ConfigRepository.getMaxSecondsBetweenMessagesSupplier(featureFlagClient));
    val configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    val configDatabaseInitializer = DatabaseCheckFactory.createConfigsDatabaseInitializer(configsDslContext,
        configsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH));
    val configsDatabaseMigrator = new ConfigsDatabaseMigrator(configDatabase, configsFlyway);
    final Optional<DefinitionsProvider> definitionsProvider =
        Optional.of(new LocalDefinitionsProvider());
    val jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    val jobsDatabaseInitializer = DatabaseCheckFactory.createJobsDatabaseInitializer(jobsDslContext,
        jobsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH));
    val jobsDatabaseMigrator = new JobsDatabaseMigrator(jobDatabase, jobsFlyway);
    val jobsPersistence = new DefaultJobPersistence(jobDatabase);
    val protocolVersionChecker = new ProtocolVersionChecker(jobsPersistence, airbyteProtocolRange, configRepository, definitionsProvider);
    val actorDefinitionMigrator = new ActorDefinitionMigrator(configRepository, featureFlagClient);
    val applyDefinitionsHelper = new ApplyDefinitionsHelper(actorDefinitionMigrator, definitionsProvider, jobsPersistence);
    final CdkVersionProvider cdkVersionProvider = mock(CdkVersionProvider.class);
    when(cdkVersionProvider.getCdkVersion()).thenReturn(CDK_VERSION);
    val declarativeSourceUpdater = new DeclarativeSourceUpdater(configRepository, cdkVersionProvider);
    val postLoadExecutor =
        new DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, mockedFeatureFlags, jobsPersistence);

    val bootloader =
        new Bootloader(false, configRepository, configDatabaseInitializer, configsDatabaseMigrator, currentAirbyteVersion,
            definitionsProvider, mockedFeatureFlags, jobsDatabaseInitializer, jobsDatabaseMigrator, jobsPersistence, protocolVersionChecker,
            runMigrationOnStartup, postLoadExecutor);
    bootloader.load();

    val jobsMigrator = new JobsDatabaseMigrator(jobDatabase, jobsFlyway);
    assertEquals(CURRENT_JOBS_MIGRATION_VERSION, jobsMigrator.getLatestMigration().getVersion().getVersion());

    val configsMigrator = new ConfigsDatabaseMigrator(configDatabase, configsFlyway);
    assertEquals(CURRENT_CONFIGS_MIGRATION_VERSION, configsMigrator.getLatestMigration().getVersion().getVersion());

    assertEquals(VERSION_0330_ALPHA, jobsPersistence.getVersion().get());
    assertEquals(new Version(PROTOCOL_VERSION_123), jobsPersistence.getAirbyteProtocolVersionMin().get());
    assertEquals(new Version(PROTOCOL_VERSION_124), jobsPersistence.getAirbyteProtocolVersionMax().get());

    assertNotEquals(Optional.empty(), jobsPersistence.getDeployment());
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  void testRequiredVersionUpgradePredicate() throws Exception {
    val currentAirbyteVersion = new AirbyteVersion(VERSION_0330_ALPHA);
    val airbyteProtocolRange = new AirbyteProtocolVersionRange(new Version(PROTOCOL_VERSION_123), new Version(PROTOCOL_VERSION_124));
    val mockedFeatureFlags = mock(FeatureFlags.class);
    val runMigrationOnStartup = true;

    val configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);
    val jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);

    val configsFlyway = createConfigsFlyway(configsDataSource);
    val jobsFlyway = createJobsFlyway(jobsDataSource);

    val configDatabase = new ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false);
    val jobDatabase = new JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false);
    val configRepository = new ConfigRepository(configDatabase, ConfigRepository.getMaxSecondsBetweenMessagesSupplier(featureFlagClient));
    val configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    val configDatabaseInitializer = DatabaseCheckFactory.createConfigsDatabaseInitializer(configsDslContext,
        configsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH));
    val configsDatabaseMigrator = new ConfigsDatabaseMigrator(configDatabase, configsFlyway);
    final Optional<DefinitionsProvider> definitionsProvider = Optional.of(
        new LocalDefinitionsProvider());
    val jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    val jobsDatabaseInitializer = DatabaseCheckFactory.createJobsDatabaseInitializer(jobsDslContext,
        jobsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH));
    val jobsDatabaseMigrator = new JobsDatabaseMigrator(jobDatabase, jobsFlyway);
    val jobsPersistence = new DefaultJobPersistence(jobDatabase);
    val protocolVersionChecker = new ProtocolVersionChecker(jobsPersistence, airbyteProtocolRange, configRepository, definitionsProvider);
    val actorDefinitionMigrator = new ActorDefinitionMigrator(configRepository, featureFlagClient);
    val applyDefinitionsHelper = new ApplyDefinitionsHelper(actorDefinitionMigrator, definitionsProvider, jobsPersistence);
    final CdkVersionProvider cdkVersionProvider = mock(CdkVersionProvider.class);
    when(cdkVersionProvider.getCdkVersion()).thenReturn(CDK_VERSION);
    val declarativeSourceUpdater = new DeclarativeSourceUpdater(configRepository, cdkVersionProvider);
    val postLoadExecutor =
        new DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, mockedFeatureFlags, jobsPersistence);

    val bootloader =
        new Bootloader(false, configRepository, configDatabaseInitializer, configsDatabaseMigrator, currentAirbyteVersion,
            definitionsProvider, mockedFeatureFlags, jobsDatabaseInitializer, jobsDatabaseMigrator, jobsPersistence, protocolVersionChecker,
            runMigrationOnStartup, postLoadExecutor);

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
    val currentAirbyteVersion = new AirbyteVersion(VERSION_0330_ALPHA);
    val airbyteProtocolRange = new AirbyteProtocolVersionRange(new Version(PROTOCOL_VERSION_123), new Version(PROTOCOL_VERSION_124));
    val mockedFeatureFlags = mock(FeatureFlags.class);
    val runMigrationOnStartup = true;

    val configsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);
    val jobsDslContext = DSLContextFactory.create(configsDataSource, SQLDialect.POSTGRES);

    val configsFlyway = createConfigsFlyway(configsDataSource);
    val jobsFlyway = createJobsFlyway(jobsDataSource);

    val configDatabase = new ConfigsDatabaseTestProvider(configsDslContext, configsFlyway).create(false);
    val jobDatabase = new JobsDatabaseTestProvider(jobsDslContext, jobsFlyway).create(false);
    val configRepository = new ConfigRepository(configDatabase, ConfigRepository.getMaxSecondsBetweenMessagesSupplier(featureFlagClient));
    val configsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    val configDatabaseInitializer = DatabaseCheckFactory.createConfigsDatabaseInitializer(configsDslContext,
        configsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH));
    val configsDatabaseMigrator = new ConfigsDatabaseMigrator(configDatabase, configsFlyway);
    final Optional<DefinitionsProvider> definitionsProvider =
        Optional.of(new LocalDefinitionsProvider());
    val jobsDatabaseInitializationTimeoutMs = TimeUnit.SECONDS.toMillis(60L);
    val jobsDatabaseInitializer = DatabaseCheckFactory.createJobsDatabaseInitializer(jobsDslContext,
        jobsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH));
    val jobsDatabaseMigrator = new JobsDatabaseMigrator(jobDatabase, jobsFlyway);
    val jobsPersistence = new DefaultJobPersistence(jobDatabase);
    val protocolVersionChecker = new ProtocolVersionChecker(jobsPersistence, airbyteProtocolRange, configRepository, definitionsProvider);
    val postLoadExecutor = new PostLoadExecutor() {

      @Override
      public void execute() {
        testTriggered.set(true);
      }

    };
    val bootloader =
        new Bootloader(false, configRepository, configDatabaseInitializer, configsDatabaseMigrator, currentAirbyteVersion,
            definitionsProvider, mockedFeatureFlags, jobsDatabaseInitializer, jobsDatabaseMigrator, jobsPersistence, protocolVersionChecker,
            runMigrationOnStartup, postLoadExecutor);
    bootloader.load();
    assertTrue(testTriggered.get());
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
