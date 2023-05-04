/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories;

import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.db.instance.jobs.jooq.generated.Keys;
import io.airbyte.db.instance.jobs.jooq.generated.Tables;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState;
import io.airbyte.db.instance.test.TestDatabaseProviders;
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams;
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams.FilterParamsBuilder;
import io.airbyte.server.repositories.StreamStatusesRepository.Pagination;
import io.airbyte.server.repositories.domain.StreamStatus;
import io.airbyte.server.repositories.domain.StreamStatus.StreamStatusBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.transaction.jdbc.DelegatingDataSource;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
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

@SuppressWarnings("MissingJavadocType")
@MicronautTest
class StreamStatusesRepositoryTest {

  static ApplicationContext context;

  static StreamStatusesRepository repo;

  static DSLContext jooqDslContext;

  // we run against an actual database to ensure hibernate and jooq properly integrate
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
            "datasources.default.driverClassName", "org.postgresql.Driver",
            "datasources.default.db-type", "postgres",
            "datasources.default.dialect", "POSTGRES",
            "datasources.default.url", container.getJdbcUrl(),
            "datasources.default.username", container.getUsername(),
            "datasources.default.password", container.getPassword())));

    // removes micronaut transactional wrapper that doesn't play nice with our non-micronaut factories
    final var dataSource = ((DelegatingDataSource) context.getBean(DataSource.class)).getTargetDataSource();
    jooqDslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
    final var databaseProviders = new TestDatabaseProviders(dataSource, jooqDslContext);

    // this line is what runs the migrations
    databaseProviders.createNewJobsDatabase();

    // so we don't have to deal with making jobs as well
    jooqDslContext.alterTable(Tables.STREAM_STATUSES).dropForeignKey(Keys.STREAM_STATUSES__STREAM_STATUSES_JOB_ID_FKEY.constraint()).execute();

    repo = context.getBean(StreamStatusesRepository.class);
  }

  @BeforeEach
  void truncate() {
    jooqDslContext.truncateTable(Tables.STREAM_STATUSES).cascade().execute();
  }

  @AfterAll
  static void dbDown() {
    container.close();
  }

  @Test
  void testInsert() {
    final var s = Fixtures.status().build();

    final var inserted = repo.save(s);

    final var found = repo.findById(inserted.getId());

    Assertions.assertTrue(found.isPresent());
    Assertions.assertEquals(inserted, found.get());
  }

  @Test
  void testUpdateCompleteFlow() {
    final var pendingAt = Fixtures.now();
    final var s = Fixtures.status().transitionedAt(pendingAt).build();

    final var inserted = repo.save(s);
    final var id = inserted.getId();
    final var found1 = repo.findById(id);

    final var runningAt = Fixtures.now();
    final var running = Fixtures.statusFrom(inserted)
        .runState(JobStreamStatusRunState.running)
        .transitionedAt(runningAt)
        .build();
    repo.update(running);
    final var found2 = repo.findById(id);

    final var completedAt = Fixtures.now();
    final var completed = Fixtures.statusFrom(running)
        .runState(JobStreamStatusRunState.complete)
        .transitionedAt(completedAt)
        .build();
    repo.update(completed);
    final var found3 = repo.findById(id);

    Assertions.assertTrue(found1.isPresent());
    Assertions.assertEquals(pendingAt, found1.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.pending, found1.get().getRunState());

    Assertions.assertTrue(found2.isPresent());
    Assertions.assertEquals(runningAt, found2.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.running, found2.get().getRunState());

    Assertions.assertTrue(found3.isPresent());
    Assertions.assertEquals(completedAt, found3.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.complete, found3.get().getRunState());
  }

  @Test
  void testUpdateIncompleteFlowFailed() {
    final var pendingAt = Fixtures.now();
    final var s = Fixtures.status().transitionedAt(pendingAt).build();

    final var inserted = repo.save(s);
    final var id = inserted.getId();
    final var found1 = repo.findById(id);

    final var runningAt = Fixtures.now();
    final var running = Fixtures.statusFrom(inserted)
        .runState(JobStreamStatusRunState.running)
        .transitionedAt(runningAt)
        .build();
    repo.update(running);
    final var found2 = repo.findById(id);

    final var incompleteAt = Fixtures.now();
    final var incomplete = Fixtures.statusFrom(running)
        .runState(JobStreamStatusRunState.incomplete)
        .incompleteRunCause(JobStreamStatusIncompleteRunCause.failed)
        .transitionedAt(incompleteAt)
        .build();
    repo.update(incomplete);
    final var found3 = repo.findById(id);

    Assertions.assertTrue(found1.isPresent());
    Assertions.assertEquals(pendingAt, found1.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.pending, found1.get().getRunState());
    Assertions.assertNull(found1.get().getIncompleteRunCause());

    Assertions.assertTrue(found2.isPresent());
    Assertions.assertEquals(runningAt, found2.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.running, found2.get().getRunState());
    Assertions.assertNull(found2.get().getIncompleteRunCause());

    Assertions.assertTrue(found3.isPresent());
    Assertions.assertEquals(incompleteAt, found3.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.incomplete, found3.get().getRunState());
    Assertions.assertEquals(JobStreamStatusIncompleteRunCause.failed, found3.get().getIncompleteRunCause());
  }

  @Test
  void testUpdateIncompleteFlowCanceled() {
    final var pendingAt = Fixtures.now();
    final var s = Fixtures.status().transitionedAt(pendingAt).build();

    final var inserted = repo.save(s);
    final var id = inserted.getId();
    final var found1 = repo.findById(id);

    final var runningAt = Fixtures.now();
    final var running = Fixtures.statusFrom(inserted)
        .runState(JobStreamStatusRunState.running)
        .transitionedAt(runningAt)
        .build();
    repo.update(running);
    final var found2 = repo.findById(id);

    final var incompleteAt = Fixtures.now();
    final var incomplete = Fixtures.statusFrom(running)
        .runState(JobStreamStatusRunState.incomplete)
        .incompleteRunCause(JobStreamStatusIncompleteRunCause.canceled)
        .transitionedAt(incompleteAt)
        .build();
    repo.update(incomplete);
    final var found3 = repo.findById(id);

    Assertions.assertTrue(found1.isPresent());
    Assertions.assertEquals(pendingAt, found1.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.pending, found1.get().getRunState());
    Assertions.assertNull(found1.get().getIncompleteRunCause());

    Assertions.assertTrue(found2.isPresent());
    Assertions.assertEquals(runningAt, found2.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.running, found2.get().getRunState());
    Assertions.assertNull(found2.get().getIncompleteRunCause());

    Assertions.assertTrue(found3.isPresent());
    Assertions.assertEquals(incompleteAt, found3.get().getTransitionedAt());
    Assertions.assertEquals(JobStreamStatusRunState.incomplete, found3.get().getRunState());
    Assertions.assertEquals(JobStreamStatusIncompleteRunCause.canceled, found3.get().getIncompleteRunCause());
  }

  @Test
  void testFindAllFilteredSimple() {
    final var s1 = Fixtures.status().workspaceId(Fixtures.workspaceId1).build();
    final var s2 = Fixtures.status().workspaceId(Fixtures.workspaceId2).build();
    final var inserted1 = repo.save(s1);
    final var inserted2 = repo.save(s2);

    final var result = repo.findAllFiltered(new FilterParams(Fixtures.workspaceId1, null, null, null, null, null, null));

    Assertions.assertEquals(1, result.getContent().size());
    Assertions.assertEquals(inserted1.getId(), result.getContent().get(0).getId());
    Assertions.assertNotEquals(inserted2.getId(), result.getContent().get(0).getId());
  }

  @Test
  void testFindAllFilteredMatrix() {
    // create and save a variety of stream statuses
    final var s1 = Fixtures.status().build();
    final var s2 = Fixtures.statusFrom(s1).attemptNumber(1).build();
    final var s3 = Fixtures.statusFrom(s2).attemptNumber(2).build();
    final var s4 = Fixtures.status().streamName(Fixtures.testName2).build();
    final var s5 = Fixtures.statusFrom(s4).attemptNumber(1).build();
    final var s6 = Fixtures.status().workspaceId(Fixtures.workspaceId2).build();
    final var s7 = Fixtures.status().workspaceId(Fixtures.workspaceId3).build();
    final var s8 = Fixtures.status().jobId(Fixtures.jobId2).build();
    final var s9 = Fixtures.statusFrom(s8).attemptNumber(1).build();
    final var s10 = Fixtures.statusFrom(s8).streamName(Fixtures.testName3).build();
    final var s11 = Fixtures.status().connectionId(Fixtures.connectionId2).build();
    final var s12 = Fixtures.status().connectionId(Fixtures.connectionId3).build();
    final var s13 = Fixtures.status().streamNamespace("").build();

    repo.saveAll(List.of(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13));

    // create some filter params on various properties
    final var f1 = Fixtures.filters().workspaceId(Fixtures.workspaceId1).build();
    final var f2 = Fixtures.filters().workspaceId(Fixtures.workspaceId2).build();
    final var f3 = Fixtures.filters().workspaceId(Fixtures.workspaceId3).build();
    final var f4 = Fixtures.filters().connectionId(Fixtures.connectionId1).build();
    final var f5 = Fixtures.filters().connectionId(Fixtures.connectionId2).build();
    final var f6 = Fixtures.filters().connectionId(Fixtures.connectionId1).streamNamespace(Fixtures.testNamespace).build();
    final var f7 =
        Fixtures.filters().connectionId(Fixtures.connectionId1).streamNamespace(Fixtures.testNamespace).streamName(Fixtures.testName1).build();
    final var f8 = Fixtures.filters().jobId(Fixtures.jobId1).build();
    final var f9 = Fixtures.filters().jobId(Fixtures.jobId2).build();
    final var f10 = Fixtures.filters().jobId(Fixtures.jobId3).build();
    final var f11 = Fixtures.filters().jobId(Fixtures.jobId1).streamNamespace(Fixtures.testNamespace).streamName(Fixtures.testName1).build();
    final var f12 = Fixtures.filters().jobId(Fixtures.jobId1).streamNamespace(Fixtures.testNamespace).streamName(Fixtures.testName2).build();
    final var f13 =
        Fixtures.filters().jobId(Fixtures.jobId1).streamNamespace(Fixtures.testNamespace).streamName(Fixtures.testName1).attemptNumber(2).build();

    Assertions.assertEquals(repo.findAllFiltered(f1).getContent(), List.of(s1, s2, s3, s4, s5, s8, s9, s10, s11, s12, s13));
    Assertions.assertEquals(repo.findAllFiltered(f2).getContent(), List.of(s6));
    Assertions.assertEquals(repo.findAllFiltered(f3).getContent(), List.of(s7));
    Assertions.assertEquals(repo.findAllFiltered(f4).getContent(), List.of(s1, s2, s3, s4, s5, s8, s9, s10, s13));
    Assertions.assertEquals(repo.findAllFiltered(f5).getContent(), List.of(s11));
    Assertions.assertEquals(repo.findAllFiltered(f6).getContent(), List.of(s1, s2, s3, s4, s5, s8, s9, s10));
    Assertions.assertEquals(repo.findAllFiltered(f7).getContent(), List.of(s1, s2, s3, s8, s9));
    Assertions.assertEquals(repo.findAllFiltered(f8).getContent(), List.of(s1, s2, s3, s4, s5, s11, s12, s13));
    Assertions.assertEquals(repo.findAllFiltered(f9).getContent(), List.of(s8, s9, s10));
    Assertions.assertEquals(repo.findAllFiltered(f10).getContent(), List.of());
    Assertions.assertEquals(repo.findAllFiltered(f11).getContent(), List.of(s1, s2, s3, s11, s12));
    Assertions.assertEquals(repo.findAllFiltered(f12).getContent(), List.of(s4, s5));
    Assertions.assertEquals(repo.findAllFiltered(f13).getContent(), List.of(s3));
  }

  @Test
  void testPagination() {
    // create 10 statuses
    final var s1 = Fixtures.status().build();
    final var s2 = Fixtures.status().build();
    final var s3 = Fixtures.status().build();
    final var s4 = Fixtures.status().build();
    final var s5 = Fixtures.status().build();
    final var s6 = Fixtures.status().build();
    final var s7 = Fixtures.status().build();
    final var s8 = Fixtures.status().build();
    final var s9 = Fixtures.status().build();
    final var s10 = Fixtures.status().build();

    repo.saveAll(List.of(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10));

    // paginate by 10 at a time
    final var f1 = Fixtures.filters().pagination(new Pagination(0, 10)).build();
    final var f2 = Fixtures.filters().pagination(new Pagination(1, 10)).build();

    // paginate by 5
    final var f3 = Fixtures.filters().pagination(new Pagination(0, 5)).build();
    final var f4 = Fixtures.filters().pagination(new Pagination(1, 5)).build();
    final var f5 = Fixtures.filters().pagination(new Pagination(2, 5)).build();

    // paginate by 3
    final var f6 = Fixtures.filters().pagination(new Pagination(0, 3)).build();
    final var f7 = Fixtures.filters().pagination(new Pagination(1, 3)).build();
    final var f8 = Fixtures.filters().pagination(new Pagination(2, 3)).build();
    final var f9 = Fixtures.filters().pagination(new Pagination(3, 3)).build();
    final var f10 = Fixtures.filters().pagination(new Pagination(4, 3)).build();

    Assertions.assertEquals(repo.findAllFiltered(f1).getContent(), List.of(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10));
    Assertions.assertEquals(repo.findAllFiltered(f2).getContent(), List.of());

    Assertions.assertEquals(repo.findAllFiltered(f3).getContent(), List.of(s1, s2, s3, s4, s5));
    Assertions.assertEquals(repo.findAllFiltered(f4).getContent(), List.of(s6, s7, s8, s9, s10));
    Assertions.assertEquals(repo.findAllFiltered(f5).getContent(), List.of());

    Assertions.assertEquals(repo.findAllFiltered(f6).getContent(), List.of(s1, s2, s3));
    Assertions.assertEquals(repo.findAllFiltered(f7).getContent(), List.of(s4, s5, s6));
    Assertions.assertEquals(repo.findAllFiltered(f8).getContent(), List.of(s7, s8, s9));
    Assertions.assertEquals(repo.findAllFiltered(f9).getContent(), List.of(s10));
    Assertions.assertEquals(repo.findAllFiltered(f10).getContent(), List.of());
  }

  private static class Fixtures {

    static String testNamespace = "test_";

    static String testName1 = "table_1";
    static String testName2 = "table_2";
    static String testName3 = "table_3";

    static UUID workspaceId1 = UUID.randomUUID();
    static UUID workspaceId2 = UUID.randomUUID();
    static UUID workspaceId3 = UUID.randomUUID();

    static UUID connectionId1 = UUID.randomUUID();
    static UUID connectionId2 = UUID.randomUUID();
    static UUID connectionId3 = UUID.randomUUID();

    static Long jobId1 = ThreadLocalRandom.current().nextLong();
    static Long jobId2 = ThreadLocalRandom.current().nextLong();
    static Long jobId3 = ThreadLocalRandom.current().nextLong();

    // java defaults to 9 precision while postgres defaults to 6
    // this provides us with 6 decimal precision for comparison purposes
    static OffsetDateTime now() {
      return OffsetDateTime.ofInstant(
          Instant.now().truncatedTo(ChronoUnit.MICROS),
          ZoneId.systemDefault());
    }

    static StreamStatusBuilder status() {
      return StreamStatus.builder()
          .workspaceId(workspaceId1)
          .connectionId(connectionId1)
          .jobId(jobId1)
          .attemptNumber(0)
          .streamNamespace(testNamespace)
          .streamName(testName1)
          .jobType(JobStreamStatusJobType.sync)
          .runState(JobStreamStatusRunState.pending)
          .transitionedAt(now());
    }

    static StreamStatusBuilder statusFrom(final StreamStatus s) {
      return StreamStatus.builder()
          .id(s.getId())
          .workspaceId(s.getWorkspaceId())
          .connectionId(s.getConnectionId())
          .jobId(s.getJobId())
          .attemptNumber(s.getAttemptNumber())
          .streamNamespace(s.getStreamNamespace())
          .streamName(s.getStreamName())
          .jobType(s.getJobType())
          .runState(s.getRunState())
          .incompleteRunCause(s.getIncompleteRunCause())
          .createdAt(s.getCreatedAt())
          .updatedAt(s.getUpdatedAt())
          .transitionedAt(s.getTransitionedAt());
    }

    static FilterParamsBuilder filters() {
      return FilterParams.builder()
          .workspaceId(workspaceId1);
    }

  }

}
