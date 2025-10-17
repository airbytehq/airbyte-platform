/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.bootloader.runtime.AirbyteBootloaderConfig
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.resources.Resources
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.init.PostLoadExecutor
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.db.init.DatabaseInitializer
import io.airbyte.db.instance.DatabaseMigrator
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.persistence.job.JobPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

private val log = KotlinLogging.logger {}

/**
 * Ensures that the databases are migrated to the appropriate level.
 */
@Singleton
class Bootloader(
  private val airbyteBootloaderConfig: AirbyteBootloaderConfig,
  private val airbyteAuthConfig: AirbyteAuthConfig,
  private val workspaceService: WorkspaceService,
  @param:Named("configsDatabaseInitializer") private val configsDatabaseInitializer: DatabaseInitializer,
  @param:Named("configsDatabaseMigrator") private val configsDatabaseMigrator: DatabaseMigrator,
  private val currentAirbyteVersion: AirbyteVersion,
  @param:Named("jobsDatabaseInitializer") private val jobsDatabaseInitializer: DatabaseInitializer,
  @param:Named("jobsDatabaseMigrator") private val jobsDatabaseMigrator: DatabaseMigrator,
  private val jobPersistence: JobPersistence,
  private val organizationService: OrganizationService,
  private val ssoConfigService: SsoConfigService,
  private val protocolVersionChecker: ProtocolVersionChecker,
  private val postLoadExecution: PostLoadExecutor?,
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataplaneInitializer: DataplaneInitializer,
  val airbyteEdition: AirbyteEdition,
  private val authSecretInitializer: AuthKubernetesSecretInitializer?,
  private val secretStorageInitializer: SecretStorageInitializer,
) {
  /**
   * Performs all required bootstrapping for the Airbyte environment. This includes the following:
   *
   *  * Initializes the databases
   *  * Check database migration compatibility
   *  * Check protocol version compatibility
   *  * Migrate databases
   *  * Create default workspace
   *  * Create default deployment
   *  * Perform post migration tasks
   *
   * @throws Exception if unable to perform any of the bootstrap operations.
   */
  fun load() {
    if (authSecretInitializer != null) {
      log.info { "Initializing auth secrets..." }
      authSecretInitializer.checkAccessToSecrets(currentAirbyteVersion)
      authSecretInitializer.initializeSecrets()
    }

    log.info { "Initializing databases..." }
    initializeDatabases()

    log.info { "Checking migration compatibility..." }
    assertNonBreakingMigration(jobPersistence, currentAirbyteVersion)

    log.info { "Checking protocol version constraints..." }
    assertNonBreakingProtocolVersionConstraints(protocolVersionChecker, jobPersistence, airbyteBootloaderConfig.autoUpgradeConnectors)

    log.info { "Running database migrations..." }
    runFlywayMigration(airbyteBootloaderConfig.runMigrationAtStartup, configsDatabaseMigrator, jobsDatabaseMigrator)

    log.info { "Registering dataplane (if none exists)..." }
    dataplaneInitializer.createDataplaneIfNotExists()

    log.info { "Creating workspace (if none exists)..." }
    createWorkspaceIfNoneExists(workspaceService)

    log.info { "Creating deployment (if none exists)..." }
    createDeploymentIfNoneExists(jobPersistence)

    log.info { "assign default organization to sso realm config..." }
    if (airbyteEdition != AirbyteEdition.CLOUD) {
      createSsoConfigForDefaultOrgIfNoneExists()
    }

    log.info { "Initializing default secret storage..." }
    secretStorageInitializer.createOrUpdateDefaultSecretStorage()

    val airbyteVersion = currentAirbyteVersion.serialize()
    log.info { "Setting Airbyte version to '$airbyteVersion'" }
    jobPersistence.setVersion(airbyteVersion)
    log.info { "Set version to '$airbyteVersion'" }

    postLoadExecution?.execute()?.also {
      log.info { "Finished running post load Execution." }
    }

    log.info { "Finished bootstrapping Airbyte environment." }
  }

  private fun assertNonBreakingMigration(
    jobPersistence: JobPersistence,
    airbyteVersion: AirbyteVersion,
  ) {
    // version in the database when the server main method is called. may be empty if this is the first
    // time the server is started.
    log.info { "Checking for illegal upgrade..." }
    val initialAirbyteDatabaseVersion = jobPersistence.getVersion().map { version: String -> AirbyteVersion(version) }
    val requiredVersionUpgrade = getRequiredVersionUpgrade(initialAirbyteDatabaseVersion.orElse(null), airbyteVersion)
    if (requiredVersionUpgrade != null) {
      val attentionBanner = Resources.read("banner/attention-banner.txt")
      log.error { attentionBanner }
      val message =
        "Cannot upgrade from version ${initialAirbyteDatabaseVersion.get().serialize()} to version ${airbyteVersion.serialize()} " +
          "directly. First you must upgrade to version ${requiredVersionUpgrade.serialize()}. After that upgrade is complete, you may upgrade to " +
          "version ${airbyteVersion.serialize()}."

      log.error { message }
      throw RuntimeException(message)
    }
  }

  private fun assertNonBreakingProtocolVersionConstraints(
    protocolVersionChecker: ProtocolVersionChecker,
    jobPersistence: JobPersistence,
    autoUpgradeConnectors: Boolean,
  ) {
    val newProtocolRange =
      protocolVersionChecker.validate(autoUpgradeConnectors)
        ?: throw RuntimeException(
          "Aborting bootloader to avoid breaking existing connection after an upgrade. " +
            "Please address airbyte protocol version support issues in the connectors before retrying.",
        )
    trackProtocolVersion(jobPersistence, newProtocolRange)
  }

  private fun createDeploymentIfNoneExists(jobPersistence: JobPersistence) {
    val deploymentOptional = jobPersistence.getDeployment()
    if (deploymentOptional.isPresent) {
      log.info { "Running deployment: ${deploymentOptional.get()}" }
    } else {
      val deploymentId = UUID.randomUUID()
      jobPersistence.setDeployment(deploymentId)
      log.info { "Created deployment: $deploymentId" }
    }
  }

  private fun createSsoConfigForDefaultOrgIfNoneExists() {
    val existingConfig = ssoConfigService.getSsoConfig(DEFAULT_ORGANIZATION_ID)
    if (existingConfig != null) {
      if (existingConfig.keycloakRealm != airbyteAuthConfig.defaultRealm) {
        log.info { "SsoConfig already exists for the default organization with a different realm. Deleting and recreating." }
        ssoConfigService.deleteSsoConfig(DEFAULT_ORGANIZATION_ID)
      } else {
        // Config exists with correct realm, nothing to do
        return
      }
    }

    if (ssoConfigService.getSsoConfigByRealmName(airbyteAuthConfig.defaultRealm) != null) {
      log.info {
        "An SsoConfig with realm ${airbyteAuthConfig.defaultRealm} already exists, so one cannot be created for the default organization."
      }
      return
    }

    val ssoConfig =
      io.airbyte.domain.models.SsoConfig(
        organizationId = DEFAULT_ORGANIZATION_ID,
        companyIdentifier = airbyteAuthConfig.defaultRealm,
        clientId = "",
        clientSecret = "",
        discoveryUrl = "",
        emailDomain = null,
        status = io.airbyte.domain.models.SsoConfigStatus.ACTIVE,
      )
    ssoConfigService.createSsoConfig(ssoConfig)
  }

  private fun createWorkspaceIfNoneExists(workspaceService: WorkspaceService) {
    if (!workspaceService.listStandardWorkspaces(true).isEmpty()) {
      log.info { "Workspace already exists for the deployment." }
      return
    }

    val workspaceId = UUID.randomUUID()
    val workspace =
      StandardWorkspace()
        .withWorkspaceId(workspaceId) // NOTE: we made a change to set this to the default User ID. It was reverted back to a random UUID
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
        .withDataplaneGroupId(dataplaneGroupService.getDefaultDataplaneGroup().id)
        // attach this new workspace to the Default Organization which should always exist at this point.
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
    // NOTE: it's safe to use the NoSecrets version since we know that the user hasn't supplied any
    // secrets yet.
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
  }

  private fun initializeDatabases() {
    log.info { "Initializing databases..." }
    configsDatabaseInitializer.initialize()
    jobsDatabaseInitializer.initialize()
    log.info { "Databases initialized." }
  }

  @InternalForTesting
  fun getRequiredVersionUpgrade(
    airbyteDatabaseVersion: AirbyteVersion?,
    airbyteVersion: AirbyteVersion,
  ): AirbyteVersion? {
    // means there was no previous version so no upgrade even needs to happen. always legal.
    if (airbyteDatabaseVersion == null) {
      log.info { "No previous Airbyte Version set." }
      return null
    }

    log.info { "Current Airbyte version: $airbyteDatabaseVersion" }
    log.info { "Future Airbyte version: $airbyteVersion" }

    for (version in REQUIRED_VERSION_UPGRADES) {
      val futureVersionIsAfterVersionBreak = airbyteVersion.greaterThan(version) || airbyteVersion.isDev()
      val isUpgradingThroughVersionBreak = airbyteDatabaseVersion.lessThan(version) && futureVersionIsAfterVersionBreak
      if (isUpgradingThroughVersionBreak) {
        return version
      }
    }

    return null
  }

  private fun runFlywayMigration(
    runDatabaseMigrationOnStartup: Boolean,
    configDbMigrator: DatabaseMigrator,
    jobDbMigrator: DatabaseMigrator,
  ) {
    log.info { "Creating baseline for config database..." }
    configDbMigrator.createBaseline()
    log.info { "Creating baseline for job database..." }
    jobDbMigrator.createBaseline()

    if (runDatabaseMigrationOnStartup) {
      log.info { "Migrating configs database..." }
      configDbMigrator.migrate()
      log.info { "Migrating jobs database..." }
      jobDbMigrator.migrate()
    } else {
      log.info { "Auto database migration has been skipped." }
    }
  }

  private fun trackProtocolVersion(
    jobPersistence: JobPersistence,
    protocolVersionRange: AirbyteProtocolVersionRange,
  ) {
    jobPersistence.setAirbyteProtocolVersionMin(protocolVersionRange.min)
    jobPersistence.setAirbyteProtocolVersionMax(protocolVersionRange.max)
    log.info { "AirbyteProtocol version support range: [${protocolVersionRange.min.serialize()}:${protocolVersionRange.max.serialize()}]" }
  }

  companion object {
    // Ordered list of version upgrades that must be completed before upgrading to latest.
    private val REQUIRED_VERSION_UPGRADES: List<AirbyteVersion> =
      listOf(
        AirbyteVersion("0.32.0-alpha"),
        AirbyteVersion("0.37.0-alpha"),
      )
  }
}
