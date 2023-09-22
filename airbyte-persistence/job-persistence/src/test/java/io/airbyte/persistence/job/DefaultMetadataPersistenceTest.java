/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static io.airbyte.db.instance.jobs.jooq.generated.Tables.AIRBYTE_METADATA;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.SYNC_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.instance.test.TestDatabaseProviders;
import io.airbyte.test.utils.Databases;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

public class DefaultMetadataPersistenceTest {

  private static PostgreSQLContainer<?> container;
  private Database jobDatabase;
  private Supplier<Instant> timeSupplier;
  private MetadataPersistence metadataPersistence;
  private DataSource dataSource;
  private DSLContext dslContext;

  @BeforeAll
  static void dbSetup() {
    container = new PostgreSQLContainer<>("postgres:13-alpine")
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker");
    container.start();
  }

  @AfterAll
  static void dbDown() {
    container.close();
  }

  @BeforeEach
  void setup() throws Exception {
    dataSource = Databases.createDataSource(container);
    dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
    final TestDatabaseProviders databaseProviders = new TestDatabaseProviders(dataSource, dslContext);
    jobDatabase = databaseProviders.createNewJobsDatabase();
    resetDb();

    metadataPersistence = new DefaultMetadataPersistence(jobDatabase);
  }

  @AfterEach
  void tearDown() throws Exception {
    DataSourceFactory.close(dataSource);
  }

  private void resetDb() throws SQLException {
    jobDatabase.query(ctx -> ctx.truncateTable(JOBS).cascade().execute());
    jobDatabase.query(ctx -> ctx.truncateTable(ATTEMPTS).cascade().execute());
    jobDatabase.query(ctx -> ctx.truncateTable(AIRBYTE_METADATA).cascade().execute());
    jobDatabase.query(ctx -> ctx.truncateTable(SYNC_STATS));
  }

  @Nested
  class GetAndSetDeployment {

    @Test
    void testSetDeployment() throws IOException {
      final UUID deploymentId = UUID.randomUUID();
      metadataPersistence.setDeployment(deploymentId);
      assertEquals(deploymentId, metadataPersistence.getDeployment().orElseThrow());
    }

    @Test
    void testSetDeploymentIdDoesNotReplaceExistingId() throws IOException {
      final UUID deploymentId1 = UUID.randomUUID();
      final UUID deploymentId2 = UUID.randomUUID();
      metadataPersistence.setDeployment(deploymentId1);
      metadataPersistence.setDeployment(deploymentId2);
      assertEquals(deploymentId1, metadataPersistence.getDeployment().orElseThrow());
    }

  }

  @Nested
  class GetAndSetVersion {

    @Test
    void testSetVersion() throws IOException {
      final String version = UUID.randomUUID().toString();
      metadataPersistence.setVersion(version);
      assertEquals(version, metadataPersistence.getVersion().orElseThrow());
    }

    @Test
    void testSetVersionReplacesExistingId() throws IOException {
      final String deploymentId1 = UUID.randomUUID().toString();
      final String deploymentId2 = UUID.randomUUID().toString();
      metadataPersistence.setVersion(deploymentId1);
      metadataPersistence.setVersion(deploymentId2);
      assertEquals(deploymentId2, metadataPersistence.getVersion().orElseThrow());
    }

  }

}
