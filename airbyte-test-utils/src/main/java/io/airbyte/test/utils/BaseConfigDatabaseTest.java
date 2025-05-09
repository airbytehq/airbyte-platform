/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.PERMISSION;

import io.airbyte.config.Permission;
import io.airbyte.config.persistence.PermissionPersistenceHelper;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.ConfigsDatabaseTestProvider;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.test.TestDatabaseProviders;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * This class exists to abstract away the lifecycle of the test container database and the config
 * database schema. This is ALL it intends to do. Any additional functionality belongs somewhere
 * else. It is useful for test suites that need to interact directly with the database.
 *
 * It currently lives in the test-utils module during the process of porting the persistence tests
 * from io.airbyte.config.persistence to their io.airbyte.data jooq implementation counterparts.
 * Once the porting is complete, this class should be moved to airbyte-data, and its access set back
 * to package-private.
 *
 * This class sets up a test container database and runs the config database migrations against it
 * to provide the most up-to-date schema.
 *
 * What this class is NOT designed to do:
 * <ul>
 * <li>test migration behavior, only should be used to test query behavior against the current
 * schema.</li>
 * <li>expose database details -- if you are attempting to expose container, dataSource, dslContext,
 * something is wrong.</li>
 * <li>add test fixtures or helpers--do NOT put "generic" resource helper methods (e.g.
 * createTestSource())</li>
 * </ul>
 *
 * This comment is emphatically worded, because it is tempting to add things to this class. It has
 * already happened in 3 previous iterations, and each time it takes multiple engineering days to
 * fix it.
 *
 * Usage:
 * <ul>
 * <li>Extend: Extend this class. By doing so, it will automatically create the test container db
 * and run migrations against it at the start of the test suite (@BeforeAll).</li>
 * <li>Use database: As part of the @BeforeAll the database field is set. This is the only field
 * that the extending class can access. It's lifecycle is fully managed by this class.</li>
 * <li>Reset schema: To reset the database in between tests, call truncateAllTables() as part
 * of @BeforeEach. This is the only method that this class exposes externally. It is exposed in such
 * a way, because most test suites need to declare their own @BeforeEach, so it is easier for them
 * to simply call this method there, then trying to apply a more complex inheritance scheme.</li>
 * </ul>
 *
 * Note: truncateAllTables() works by truncating each table in the db, if you add a new table, you
 * will need to add it to that method for it work as expected.
 */
@SuppressWarnings({"PMD.MutableStaticState", "PMD.SignatureDeclareThrowsException"})
public class BaseConfigDatabaseTest {

  protected static Database database;

  // keep these private, do not expose outside this class!
  private static PostgreSQLContainer<?> container;
  private static DataSource dataSource;
  private static DSLContext dslContext;

  /**
   * Create db test container, sets up java database resources, and runs migrations. Should not be
   * called externally. It is not private because junit cannot access private methods.
   *
   * @throws DatabaseInitializationException - db fails to initialize
   * @throws IOException - failure when interacting with db.
   */
  @BeforeAll
  static void dbSetup() throws DatabaseInitializationException, IOException {
    createDbContainer();
    setDb();
    migrateDb();
  }

  /**
   * Close all resources (container, data source, dsl context, database). Should not be called
   * externally. It is not private because junit cannot access private methods.
   *
   * @throws Exception - exception while closing resources
   */
  @AfterAll
  static void dbDown() throws Exception {
    DataSourceFactory.close(dataSource);
    container.close();
  }

  /**
   * Truncates tables to reset them. Designed to be used in between tests.
   *
   * Note: NEW TABLES -- When a new table is added to the db, it will need to be added here.
   *
   * @throws SQLException - failure in truncate query.
   */
  protected static void truncateAllTables() throws SQLException {
    database.query(ctx -> ctx
        .execute(
            """
            TRUNCATE TABLE
              active_declarative_manifest,
              actor,
              actor_catalog,
              actor_catalog_fetch_event,
              actor_definition,
              actor_definition_breaking_change,
              actor_definition_config_injection,
              actor_definition_version,
              actor_definition_workspace_grant,
              actor_oauth_parameter,
              auth_user,
              commands,
              connection,
              connection_operation,
              connection_timeline_event,
              connection_tag,
              connector_builder_project,
              connector_rollout,
              config_template,
              connection_template,
              dataplane,
              dataplane_client_credentials,
              dataplane_group,
              declarative_manifest,
              notification_configuration,
              operation,
              organization,
              organization_email_domain,
              organization_payment_config,
              partial_user_config,
              permission,
              schema_management,
              sso_config,
              state,
              stream_generation,
              stream_refreshes,
              stream_reset,
              tag,
              \"user\",
              user_invitation,
              workspace,
              workspace_service_account
            """));
  }

  /**
   * This method used to live on PermissionPersistence, but it was deprecated in favor of the new
   * PermissionService backed by a Micronaut Data repository. Many tests depended on this method, so
   * rather than keep it in the deprecated PermissionPersistence, a simplified version is implemented
   * here for tests only.
   */
  protected static void writePermission(final Permission permission) throws SQLException {
    final io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType permissionType =
        PermissionPersistenceHelper.convertConfigPermissionTypeToJooqPermissionType(permission.getPermissionType());

    final OffsetDateTime timestamp = OffsetDateTime.now();

    database.query(ctx -> ctx
        .insertInto(Tables.PERMISSION)
        .set(PERMISSION.ID, permission.getPermissionId())
        .set(PERMISSION.PERMISSION_TYPE, permissionType)
        .set(PERMISSION.USER_ID, permission.getUserId())
        .set(PERMISSION.WORKSPACE_ID, permission.getWorkspaceId())
        .set(PERMISSION.ORGANIZATION_ID, permission.getOrganizationId())
        .set(PERMISSION.CREATED_AT, timestamp)
        .set(PERMISSION.UPDATED_AT, timestamp)
        .execute());
  }

  private static void createDbContainer() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker");
    container.start();
  }

  private static void setDb() throws DatabaseInitializationException, IOException {
    dataSource = Databases.createDataSource(container);
    dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
    final TestDatabaseProviders databaseProviders = new TestDatabaseProviders(dataSource, dslContext);
    database = databaseProviders.createNewConfigsDatabase();
    databaseProviders.createNewJobsDatabase();
  }

  private static void migrateDb() throws IOException, DatabaseInitializationException {
    final Flyway flyway = FlywayFactory.create(
        dataSource,
        "BaseConfigDatabaseTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    new ConfigsDatabaseTestProvider(dslContext, flyway).create(true);
  }

}
