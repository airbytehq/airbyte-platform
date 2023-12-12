/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Geography;
import io.airbyte.config.SsoConfig;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.init.PostLoadExecutor;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.db.init.DatabaseInitializer;
import io.airbyte.db.instance.DatabaseMigrator;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Ensures that the databases are migrated to the appropriate level.
 */
@Singleton
@Slf4j
public class Bootloader {

  // Ordered list of version upgrades that must be completed before upgrading to latest.
  private static final List<AirbyteVersion> REQUIRED_VERSION_UPGRADES = List.of(
      new AirbyteVersion("0.32.0-alpha"),
      new AirbyteVersion("0.37.0-alpha"));

  private final boolean autoUpgradeConnectors;
  private final ConfigRepository configRepository;
  private final DatabaseMigrator configsDatabaseMigrator;
  private final DatabaseInitializer configsDatabaseInitializer;
  private final AirbyteVersion currentAirbyteVersion;
  private final FeatureFlags featureFlags;
  private final DatabaseInitializer jobsDatabaseInitializer;
  private final DatabaseMigrator jobsDatabaseMigrator;
  private final JobPersistence jobPersistence;
  private final OrganizationPersistence organizationPersistence;
  private final PostLoadExecutor postLoadExecution;
  private final ProtocolVersionChecker protocolVersionChecker;
  private final boolean runMigrationOnStartup;
  private final String defaultRealm;

  public Bootloader(
                    @Value("${airbyte.bootloader.auto-upgrade-connectors}") final boolean autoUpgradeConnectors,
                    final ConfigRepository configRepository,
                    @Named("configsDatabaseInitializer") final DatabaseInitializer configsDatabaseInitializer,
                    @Named("configsDatabaseMigrator") final DatabaseMigrator configsDatabaseMigrator,
                    final AirbyteVersion currentAirbyteVersion,
                    final FeatureFlags featureFlags,
                    @Named("jobsDatabaseInitializer") final DatabaseInitializer jobsDatabaseInitializer,
                    @Named("jobsDatabaseMigrator") final DatabaseMigrator jobsDatabaseMigrator,
                    final JobPersistence jobPersistence,
                    final OrganizationPersistence organizationPersistence,
                    final ProtocolVersionChecker protocolVersionChecker,
                    @Value("${airbyte.bootloader.run-migration-on-startup}") final boolean runMigrationOnStartup,
                    @Value("${airbyte.auth.default-realm}") final String defaultRealm,
                    final PostLoadExecutor postLoadExecution) {
    this.autoUpgradeConnectors = autoUpgradeConnectors;
    this.configRepository = configRepository;
    this.configsDatabaseInitializer = configsDatabaseInitializer;
    this.configsDatabaseMigrator = configsDatabaseMigrator;
    this.currentAirbyteVersion = currentAirbyteVersion;
    this.featureFlags = featureFlags;
    this.jobsDatabaseInitializer = jobsDatabaseInitializer;
    this.jobsDatabaseMigrator = jobsDatabaseMigrator;
    this.jobPersistence = jobPersistence;
    this.organizationPersistence = organizationPersistence;
    this.protocolVersionChecker = protocolVersionChecker;
    this.runMigrationOnStartup = runMigrationOnStartup;
    this.defaultRealm = defaultRealm;
    this.postLoadExecution = postLoadExecution;
  }

  /**
   * Performs all required bootstrapping for the Airbyte environment. This includes the following:
   * <ul>
   * <li>Initializes the databases</li>
   * <li>Check database migration compatibility</li>
   * <li>Check protocol version compatibility</li>
   * <li>Migrate databases</li>
   * <li>Create default workspace</li>
   * <li>Create default deployment</li>
   * <li>Perform post migration tasks</li>
   * </ul>
   *
   * @throws Exception if unable to perform any of the bootstrap operations.
   */
  public void load() throws Exception {
    log.info("Initializing databases...");
    initializeDatabases();

    log.info("Checking migration compatibility...");
    assertNonBreakingMigration(jobPersistence, currentAirbyteVersion);

    log.info("Checking protocol version constraints...");
    assertNonBreakingProtocolVersionConstraints(protocolVersionChecker, jobPersistence, autoUpgradeConnectors);

    log.info("Running database migrations...");
    runFlywayMigration(runMigrationOnStartup, configsDatabaseMigrator, jobsDatabaseMigrator);

    log.info("Creating workspace (if none exists)...");
    createWorkspaceIfNoneExists(configRepository);

    log.info("Creating deployment (if none exists)...");
    createDeploymentIfNoneExists(jobPersistence);

    log.info("assign default organization to sso realm config...");
    createSsoConfigForDefaultOrgIfNoneExists(organizationPersistence);

    final String airbyteVersion = currentAirbyteVersion.serialize();
    log.info("Setting Airbyte version to '{}'...", airbyteVersion);
    jobPersistence.setVersion(airbyteVersion);
    log.info("Set version to '{}'", airbyteVersion);

    if (postLoadExecution != null) {
      postLoadExecution.execute();
      log.info("Finished running post load Execution.");
    }

    log.info("Finished bootstrapping Airbyte environment.");
  }

  private void assertNonBreakingMigration(final JobPersistence jobPersistence, final AirbyteVersion airbyteVersion)
      throws IOException {
    // version in the database when the server main method is called. may be empty if this is the first
    // time the server is started.
    log.info("Checking for illegal upgrade...");
    final Optional<AirbyteVersion> initialAirbyteDatabaseVersion = jobPersistence.getVersion().map(AirbyteVersion::new);
    final Optional<AirbyteVersion> requiredVersionUpgrade = getRequiredVersionUpgrade(initialAirbyteDatabaseVersion.orElse(null), airbyteVersion);
    if (requiredVersionUpgrade.isPresent()) {
      final String attentionBanner = MoreResources.readResource("banner/attention-banner.txt");
      log.error(attentionBanner);
      final String message = String.format(
          "Cannot upgrade from version %s to version %s directly. First you must upgrade to version %s. "
              + "After that upgrade is complete, you may upgrade to version %s.",
          initialAirbyteDatabaseVersion.get().serialize(),
          airbyteVersion.serialize(),
          requiredVersionUpgrade.get().serialize(),
          airbyteVersion.serialize());

      log.error(message);
      throw new RuntimeException(message);
    }
  }

  private void assertNonBreakingProtocolVersionConstraints(final ProtocolVersionChecker protocolVersionChecker,
                                                           final JobPersistence jobPersistence,
                                                           final boolean autoUpgradeConnectors)
      throws Exception {
    final Optional<AirbyteProtocolVersionRange> newProtocolRange = protocolVersionChecker.validate(autoUpgradeConnectors);
    if (newProtocolRange.isEmpty()) {
      throw new RuntimeException(
          "Aborting bootloader to avoid breaking existing connection after an upgrade. "
              + "Please address airbyte protocol version support issues in the connectors before retrying.");
    }
    trackProtocolVersion(jobPersistence, newProtocolRange.get());
  }

  private void createDeploymentIfNoneExists(final JobPersistence jobPersistence) throws IOException {
    final Optional<UUID> deploymentOptional = jobPersistence.getDeployment();
    if (deploymentOptional.isPresent()) {
      log.info("Running deployment: {}", deploymentOptional.get());
    } else {
      final UUID deploymentId = UUID.randomUUID();
      jobPersistence.setDeployment(deploymentId);
      log.info("Created deployment: {}", deploymentId);
    }
  }

  private void createSsoConfigForDefaultOrgIfNoneExists(final OrganizationPersistence organizationPersistence) throws IOException {
    if (organizationPersistence.getSsoConfigForOrganization(OrganizationPersistence.DEFAULT_ORGANIZATION_ID).isPresent()) {
      log.info("SsoConfig already exists for the default organization.");
      return;
    }
    if (organizationPersistence.getSsoConfigByRealmName(defaultRealm).isPresent()) {
      log.info("An SsoConfig with realm {} already exists, so one cannot be created for the default organization.", defaultRealm);
      return;
    }
    organizationPersistence.createSsoConfig(new SsoConfig().withSsoConfigId(UUID.randomUUID())
        .withOrganizationId(OrganizationPersistence.DEFAULT_ORGANIZATION_ID)
        .withKeycloakRealm(defaultRealm));
  }

  private void createWorkspaceIfNoneExists(final ConfigRepository configRepository) throws JsonValidationException, IOException {
    if (!configRepository.listStandardWorkspaces(true).isEmpty()) {
      log.info("Workspace already exists for the deployment.");
      return;
    }

    final UUID workspaceId = UUID.randomUUID();
    final StandardWorkspace workspace = new StandardWorkspace()
        .withWorkspaceId(workspaceId)
        // NOTE: we made a change to set this to the default User ID. It was reverted back to a random UUID
        // because we discovered that our Segment Tracking Client uses distinct customer IDs to track the
        // number of OSS instances deployed. this is flawed because now, a single OSS instance can have
        // multiple workspaces. The long term fix is to update our analytics stack to use an instance-level
        // identifier, like deploymentId, instead of a workspace-level identifier. For a quick fix though,
        // we're reverting back to a randomized customer ID for the default workspace.
        .withCustomerId(UUID.randomUUID())
        .withName(WorkspacePersistence.DEFAULT_WORKSPACE_NAME)
        .withSlug(workspaceId.toString())
        .withInitialSetupComplete(false)
        .withDisplaySetupWizard(true)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO)
        // attach this new workspace to the Default Organization which should always exist at this point.
        .withOrganizationId(OrganizationPersistence.DEFAULT_ORGANIZATION_ID);
    // NOTE: it's safe to use the NoSecrets version since we know that the user hasn't supplied any
    // secrets yet.
    configRepository.writeStandardWorkspaceNoSecrets(workspace);
  }

  private void initializeDatabases() throws DatabaseInitializationException {
    log.info("Initializing databases...");
    configsDatabaseInitializer.initialize();
    jobsDatabaseInitializer.initialize();
    log.info("Databases initialized.");
  }

  @VisibleForTesting
  Optional<AirbyteVersion> getRequiredVersionUpgrade(@Nullable final AirbyteVersion airbyteDatabaseVersion, final AirbyteVersion airbyteVersion) {
    // means there was no previous version so upgrade even needs to happen. always legal.
    if (airbyteDatabaseVersion == null) {
      log.info("No previous Airbyte Version set.");
      return Optional.empty();
    }

    log.info("Current Airbyte version: {}", airbyteDatabaseVersion);
    log.info("Future Airbyte version: {}", airbyteVersion);

    for (final AirbyteVersion version : REQUIRED_VERSION_UPGRADES) {
      final var futureVersionIsAfterVersionBreak = airbyteVersion.greaterThan(version) || airbyteVersion.isDev();
      final var isUpgradingThroughVersionBreak = airbyteDatabaseVersion.lessThan(version) && futureVersionIsAfterVersionBreak;
      if (isUpgradingThroughVersionBreak) {
        return Optional.of(version);
      }
    }

    return Optional.empty();
  }

  private void runFlywayMigration(final boolean runDatabaseMigrationOnStartup,
                                  final DatabaseMigrator configDbMigrator,
                                  final DatabaseMigrator jobDbMigrator) {
    log.info("Creating baseline for config database...");
    configDbMigrator.createBaseline();
    log.info("Creating baseline for job database...");
    jobDbMigrator.createBaseline();

    if (runDatabaseMigrationOnStartup) {
      log.info("Migrating configs database...");
      configDbMigrator.migrate();
      log.info("Migrating jobs database...");
      jobDbMigrator.migrate();
    } else {
      log.info("Auto database migration has been skipped.");
    }
  }

  private void trackProtocolVersion(final JobPersistence jobPersistence, final AirbyteProtocolVersionRange protocolVersionRange)
      throws IOException {
    jobPersistence.setAirbyteProtocolVersionMin(protocolVersionRange.min());
    jobPersistence.setAirbyteProtocolVersionMax(protocolVersionRange.max());
    log.info("AirbyteProtocol version support range: [{}:{}]", protocolVersionRange.min().serialize(), protocolVersionRange.max().serialize());
  }

}
