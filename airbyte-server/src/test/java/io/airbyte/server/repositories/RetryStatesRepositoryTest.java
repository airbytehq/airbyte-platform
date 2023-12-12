/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories;

import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.db.instance.jobs.jooq.generated.Keys;
import io.airbyte.db.instance.jobs.jooq.generated.Tables;
import io.airbyte.db.instance.test.TestDatabaseProviders;
import io.airbyte.server.repositories.domain.RetryState;
import io.airbyte.server.repositories.domain.RetryState.RetryStateBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.transaction.jdbc.DelegatingDataSource;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

@MicronautTest
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class RetryStatesRepositoryTest {

  private static final String DATA_SOURCE_NAME = "config";
  private static final String DATA_SOURCES = "datasources.";

  static ApplicationContext context;

  static RetryStatesRepository repo;

  static DSLContext jooqDslContext;

  // we run against an actual database to ensure micronaut data and jooq properly integrate
  static PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:13-alpine")
      .withDatabaseName("airbyte")
      .withUsername("docker")
      .withPassword("docker");

  @BeforeAll
  static void setup() throws DatabaseInitializationException, IOException {
    container.start();
    // set the micronaut datasource properties to match our container we started up
    context = ApplicationContext.run(PropertySource.of(
        "test", Map.of(
            DATA_SOURCES + DATA_SOURCE_NAME + ".driverClassName", "org.postgresql.Driver",
            DATA_SOURCES + DATA_SOURCE_NAME + ".db-type", "postgres",
            DATA_SOURCES + DATA_SOURCE_NAME + ".dialect", "POSTGRES",
            DATA_SOURCES + DATA_SOURCE_NAME + ".url", container.getJdbcUrl(),
            DATA_SOURCES + DATA_SOURCE_NAME + ".username", container.getUsername(),
            DATA_SOURCES + DATA_SOURCE_NAME + ".password", container.getPassword())));

    // removes micronaut transactional wrapper that doesn't play nice with our non-micronaut factories
    final var dataSource = ((DelegatingDataSource) context.getBean(DataSource.class, Qualifiers.byName(DATA_SOURCE_NAME))).getTargetDataSource();
    jooqDslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
    final var databaseProviders = new TestDatabaseProviders(dataSource, jooqDslContext);

    // this line is what runs the migrations
    databaseProviders.createNewJobsDatabase();

    // so we don't have to deal with making jobs as well
    jooqDslContext.alterTable(Tables.RETRY_STATES).dropForeignKey(Keys.RETRY_STATES__RETRY_STATES_JOB_ID_FKEY.constraint()).execute();

    repo = context.getBean(RetryStatesRepository.class);
  }

  @BeforeEach
  void truncate() {
    jooqDslContext.truncateTable(Tables.RETRY_STATES).cascade().execute();
  }

  @AfterAll
  static void dbDown() {
    container.close();
  }

  @Test
  void testInsert() {
    final var s = Fixtures.state().build();

    final var inserted = repo.save(s);

    final var found = repo.findById(inserted.getId());

    Assertions.assertTrue(found.isPresent());
    Assertions.assertEquals(inserted, found.get());
  }

  @Test
  void testUpdateByJobId() {
    final var s = Fixtures.state()
        .jobId(Fixtures.jobId2)
        .build();

    final var inserted = repo.save(s);
    final var id = inserted.getId();
    final var found1 = repo.findById(id);

    final var updated = Fixtures.stateFrom(inserted)
        .successiveCompleteFailures(s.getSuccessiveCompleteFailures() + 1)
        .totalCompleteFailures(s.getTotalCompleteFailures() + 1)
        .successivePartialFailures(0)
        .build();

    repo.updateByJobId(Fixtures.jobId2, updated);

    final var found2 = repo.findById(id);

    Assertions.assertTrue(found1.isPresent());
    Assertions.assertEquals(s, found1.get());

    Assertions.assertTrue(found2.isPresent());
    Assertions.assertEquals(updated, found2.get());
  }

  @Test
  void findByJobId() {
    final var s1 = Fixtures.state()
        .jobId(Fixtures.jobId2)
        .connectionId(Fixtures.connectionId2)
        .totalCompleteFailures(0)
        .build();

    final var s2 = Fixtures.stateFrom(s1)
        .jobId(Fixtures.jobId3)
        .build();

    final var s3 = Fixtures.stateFrom(s2)
        .jobId(Fixtures.jobId1)
        .build();

    repo.save(s1);
    repo.save(s2);
    repo.save(s3);

    final var found1 = repo.findByJobId(Fixtures.jobId2);
    final var found2 = repo.findByJobId(Fixtures.jobId3);
    final var found3 = repo.findByJobId(Fixtures.jobId1);

    Assertions.assertTrue(found1.isPresent());
    Assertions.assertEquals(s1, found1.get());

    Assertions.assertTrue(found2.isPresent());
    Assertions.assertEquals(s2, found2.get());

    Assertions.assertTrue(found3.isPresent());
    Assertions.assertEquals(s3, found3.get());
  }

  @Test
  void testExistsByJobId() {
    final var s = Fixtures.state()
        .jobId(Fixtures.jobId3)
        .build();

    repo.save(s);

    final var exists1 = repo.existsByJobId(Fixtures.jobId3);
    final var exists2 = repo.existsByJobId(Fixtures.jobId2);

    Assertions.assertTrue(exists1);
    Assertions.assertFalse(exists2);
  }

  @Test
  void testCreateOrUpdateByJobIdUpdate() {
    final var s = Fixtures.state()
        .jobId(Fixtures.jobId2)
        .build();

    final var inserted = repo.save(s);
    final var id = inserted.getId();
    final var found1 = repo.findById(id);

    final var updated = Fixtures.stateFrom(inserted)
        .successiveCompleteFailures(s.getSuccessiveCompleteFailures() + 1)
        .totalCompleteFailures(s.getTotalCompleteFailures() + 1)
        .successivePartialFailures(0)
        .build();

    repo.createOrUpdateByJobId(Fixtures.jobId2, updated);

    final var found2 = repo.findById(id);

    Assertions.assertTrue(found1.isPresent());
    Assertions.assertEquals(s, found1.get());

    Assertions.assertTrue(found2.isPresent());
    Assertions.assertEquals(updated, found2.get());
  }

  @Test
  void testCreateOrUpdateByJobIdCreate() {
    final var s = Fixtures.state()
        .jobId(Fixtures.jobId4)
        .build();

    repo.createOrUpdateByJobId(Fixtures.jobId4, s);

    final var found1 = repo.findByJobId(Fixtures.jobId4);

    Assertions.assertTrue(found1.isPresent());
    Assertions.assertEquals(s, found1.get());
  }

  private static class Fixtures {

    static UUID connectionId1 = UUID.randomUUID();
    static UUID connectionId2 = UUID.randomUUID();

    static Long jobId1 = ThreadLocalRandom.current().nextLong();
    static Long jobId2 = ThreadLocalRandom.current().nextLong();
    static Long jobId3 = ThreadLocalRandom.current().nextLong();
    static Long jobId4 = ThreadLocalRandom.current().nextLong();

    static RetryStateBuilder state() {
      return RetryState.builder()
          .connectionId(connectionId1)
          .jobId(jobId1)
          .successiveCompleteFailures(0)
          .totalCompleteFailures(1)
          .successivePartialFailures(2)
          .totalPartialFailures(2);
    }

    static RetryStateBuilder stateFrom(final RetryState s) {
      return RetryState.builder()
          .connectionId(s.getConnectionId())
          .jobId(s.getJobId())
          .successiveCompleteFailures(s.getSuccessiveCompleteFailures())
          .totalCompleteFailures(s.getTotalCompleteFailures())
          .successivePartialFailures(s.getSuccessivePartialFailures())
          .totalPartialFailures(s.getTotalPartialFailures())
          .updatedAt(s.getUpdatedAt())
          .createdAt(s.getCreatedAt());
    }

  }

}
