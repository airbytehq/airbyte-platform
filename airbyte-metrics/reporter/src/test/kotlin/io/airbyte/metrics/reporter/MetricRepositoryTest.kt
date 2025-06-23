/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.airbyte.db.instance.jobs.jooq.generated.enums.AttemptStatus
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import org.jooq.DSLContext
import org.jooq.JSONB
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.Map
import java.util.UUID

abstract class MetricRepositoryTest {
  @BeforeEach
  fun setUp() {
    ctx!!.truncate(Tables.ACTOR).cascade().execute()
    ctx!!.truncate(Tables.CONNECTION).cascade().execute()
    ctx!!.truncate(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS).cascade().execute()
    ctx!!.truncate(io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS).cascade().execute()
    ctx!!.truncate(Tables.WORKSPACE).cascade().execute()
    ctx!!.truncate(Tables.DATAPLANE_GROUP).cascade().execute()
  }

  @Nested
  internal inner class NumJobs {
    @Test
    fun shouldReturnReleaseStages() {
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.JOB_ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.PROCESSING_TASK_QUEUE,
        ).values(10L, 1L, AttemptStatus.running, SYNC_QUEUE)
        .values(20L, 2L, AttemptStatus.running, SYNC_QUEUE)
        .values(30L, 3L, AttemptStatus.running, SYNC_QUEUE)
        .values(40L, 4L, AttemptStatus.running, AWS_SYNC_QUEUE)
        .values(50L, 5L, AttemptStatus.running, SYNC_QUEUE)
        .execute()
      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      val activeConnectionId = UUID.randomUUID()
      val inactiveConnectionId = UUID.randomUUID()
      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.STATUS,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.MANUAL,
        ).values(activeConnectionId, StatusType.active, NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true)
        .values(inactiveConnectionId, StatusType.inactive, NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true)
        .execute()

      // non-pending jobs
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(1L, activeConnectionId.toString(), JobStatus.pending)
        .execute()
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(2L, activeConnectionId.toString(), JobStatus.failed)
        .values(3L, activeConnectionId.toString(), JobStatus.running)
        .values(4L, activeConnectionId.toString(), JobStatus.running)
        .values(5L, inactiveConnectionId.toString(), JobStatus.running)
        .execute()

      Assertions.assertEquals(
        1,
        db!!.numberOfRunningJobsByTaskQueue()[SYNC_QUEUE],
      )
      Assertions.assertEquals(
        1,
        db!!.numberOfRunningJobsByTaskQueue()[AWS_SYNC_QUEUE],
      )
      // To test we send 0 for 'null' to overwrite previous bug.
      Assertions.assertEquals(0, db!!.numberOfRunningJobsByTaskQueue()["null"])
      Assertions.assertEquals(1, db!!.numberOfOrphanRunningJobs())
    }

    @Test
    @Throws(SQLException::class)
    fun runningJobsShouldReturnZero() {
      // non-pending jobs
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(1L, "", JobStatus.pending)
        .execute()
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(2L, "", JobStatus.failed)
        .execute()

      val result = db!!.numberOfRunningJobsByTaskQueue()
      Assertions.assertEquals(result[SYNC_QUEUE], 0)
      Assertions.assertEquals(result[AWS_SYNC_QUEUE], 0)
    }

    @Test
    @Throws(SQLException::class)
    fun pendingJobsShouldReturnCorrectCount() {
      // non-pending jobs
      val connectionUuid = UUID.randomUUID()
      val workspaceId = UUID.randomUUID()
      val actorDefinitionId = UUID.randomUUID()
      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .values(UUID.randomUUID(), DEFAULT_ORGANIZATION_ID, AUTO_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
        ).values(connectionUuid, NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .execute()

      ctx!!
        .insertInto(
          Tables.WORKSPACE,
          Tables.WORKSPACE.ID,
          Tables.WORKSPACE.CUSTOMER_ID,
          Tables.WORKSPACE.NAME,
          Tables.WORKSPACE.SLUG,
          Tables.WORKSPACE.EMAIL,
          Tables.WORKSPACE.INITIAL_SETUP_COMPLETE,
          Tables.WORKSPACE.ANONYMOUS_DATA_COLLECTION,
          Tables.WORKSPACE.SEND_NEWSLETTER,
          Tables.WORKSPACE.SEND_SECURITY_UPDATES,
          Tables.WORKSPACE.DISPLAY_SETUP_WIZARD,
          Tables.WORKSPACE.TOMBSTONE,
          Tables.WORKSPACE.NOTIFICATIONS,
          Tables.WORKSPACE.FIRST_SYNC_COMPLETE,
          Tables.WORKSPACE.FEEDBACK_COMPLETE,
          Tables.WORKSPACE.CREATED_AT,
          Tables.WORKSPACE.UPDATED_AT,
          Tables.WORKSPACE.WEBHOOK_OPERATION_CONFIGS,
          Tables.WORKSPACE.NOTIFICATION_SETTINGS,
          Tables.WORKSPACE.ORGANIZATION_ID,
          Tables.WORKSPACE.DATAPLANE_GROUP_ID,
        ).values(
          workspaceId,
          UUID.randomUUID(),
          "test",
          "test-slug",
          "test@example.com",
          true,
          false,
          false,
          false,
          false,
          false,
          JSONB.valueOf("{}"),
          false,
          false,
          OffsetDateTime.now(),
          OffsetDateTime.now(),
          JSONB.valueOf("{}"),
          JSONB.valueOf("{}"),
          DEFAULT_ORGANIZATION_ID,
          euDataplaneGroupId,
        ).execute()

      ctx!!
        .insertInto(
          Tables.ACTOR_DEFINITION,
          Tables.ACTOR_DEFINITION.ID,
          Tables.ACTOR_DEFINITION.NAME,
          Tables.ACTOR_DEFINITION.ACTOR_TYPE,
          Tables.ACTOR_DEFINITION.CREATED_AT,
          Tables.ACTOR_DEFINITION.UPDATED_AT,
        ).values(actorDefinitionId, "test-source-def", ActorType.source, OffsetDateTime.now(), OffsetDateTime.now())
        .execute()

      ctx!!
        .insertInto(
          Tables.ACTOR,
          Tables.ACTOR.ID,
          Tables.ACTOR.WORKSPACE_ID,
          Tables.ACTOR.ACTOR_DEFINITION_ID,
          Tables.ACTOR.NAME,
          Tables.ACTOR.CONFIGURATION,
          Tables.ACTOR.ACTOR_TYPE,
          Tables.ACTOR.TOMBSTONE,
          Tables.ACTOR.CREATED_AT,
          Tables.ACTOR.UPDATED_AT,
          Tables.ACTOR.RESOURCE_REQUIREMENTS,
        ).values(
          srcId,
          workspaceId,
          actorDefinitionId,
          "source",
          JSONB.valueOf("{}"),
          ActorType.source,
          false,
          OffsetDateTime.now(),
          OffsetDateTime.now(),
          JSONB.valueOf("{}"),
        ).execute()

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(1L, connectionUuid.toString(), JobStatus.pending)
        .values(2L, connectionUuid.toString(), JobStatus.failed)
        .values(3L, connectionUuid.toString(), JobStatus.pending)
        .values(4L, connectionUuid.toString(), JobStatus.running)
        .execute()

      val res = db!!.numberOfPendingJobsByDataplaneGroupName()
      Assertions.assertEquals(2, res[EU_REGION])
      Assertions.assertEquals(0, res[AUTO_REGION])
    }

    @Test
    @Throws(SQLException::class)
    fun pendingJobsShouldReturnZero() {
      val connectionUuid = UUID.randomUUID()
      val workspaceId = UUID.randomUUID()
      val actorDefinitionId = UUID.randomUUID()
      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .values(UUID.randomUUID(), DEFAULT_ORGANIZATION_ID, AUTO_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
        ).values(connectionUuid, NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .execute()

      ctx!!
        .insertInto(
          Tables.WORKSPACE,
          Tables.WORKSPACE.ID,
          Tables.WORKSPACE.CUSTOMER_ID,
          Tables.WORKSPACE.NAME,
          Tables.WORKSPACE.SLUG,
          Tables.WORKSPACE.EMAIL,
          Tables.WORKSPACE.INITIAL_SETUP_COMPLETE,
          Tables.WORKSPACE.ANONYMOUS_DATA_COLLECTION,
          Tables.WORKSPACE.SEND_NEWSLETTER,
          Tables.WORKSPACE.SEND_SECURITY_UPDATES,
          Tables.WORKSPACE.DISPLAY_SETUP_WIZARD,
          Tables.WORKSPACE.TOMBSTONE,
          Tables.WORKSPACE.NOTIFICATIONS,
          Tables.WORKSPACE.FIRST_SYNC_COMPLETE,
          Tables.WORKSPACE.FEEDBACK_COMPLETE,
          Tables.WORKSPACE.CREATED_AT,
          Tables.WORKSPACE.UPDATED_AT,
          Tables.WORKSPACE.WEBHOOK_OPERATION_CONFIGS,
          Tables.WORKSPACE.NOTIFICATION_SETTINGS,
          Tables.WORKSPACE.ORGANIZATION_ID,
          Tables.WORKSPACE.DATAPLANE_GROUP_ID,
        ).values(
          workspaceId,
          UUID.randomUUID(),
          "test",
          "test-slug",
          "test@example.com",
          true,
          false,
          false,
          false,
          false,
          false,
          JSONB.valueOf("{}"),
          false,
          false,
          OffsetDateTime.now(),
          OffsetDateTime.now(),
          JSONB.valueOf("{}"),
          JSONB.valueOf("{}"),
          DEFAULT_ORGANIZATION_ID,
          euDataplaneGroupId,
        ).execute()

      ctx!!
        .insertInto(
          Tables.ACTOR_DEFINITION,
          Tables.ACTOR_DEFINITION.ID,
          Tables.ACTOR_DEFINITION.NAME,
          Tables.ACTOR_DEFINITION.ACTOR_TYPE,
          Tables.ACTOR_DEFINITION.CREATED_AT,
          Tables.ACTOR_DEFINITION.UPDATED_AT,
        ).values(actorDefinitionId, "test-source-def", ActorType.source, OffsetDateTime.now(), OffsetDateTime.now())
        .execute()

      ctx!!
        .insertInto(
          Tables.ACTOR,
          Tables.ACTOR.ID,
          Tables.ACTOR.WORKSPACE_ID,
          Tables.ACTOR.ACTOR_DEFINITION_ID,
          Tables.ACTOR.NAME,
          Tables.ACTOR.CONFIGURATION,
          Tables.ACTOR.ACTOR_TYPE,
          Tables.ACTOR.TOMBSTONE,
          Tables.ACTOR.CREATED_AT,
          Tables.ACTOR.UPDATED_AT,
          Tables.ACTOR.RESOURCE_REQUIREMENTS,
        ).values(
          srcId,
          workspaceId,
          actorDefinitionId,
          "source",
          JSONB.valueOf("{}"),
          ActorType.source,
          false,
          OffsetDateTime.now(),
          OffsetDateTime.now(),
          JSONB.valueOf("{}"),
        ).execute()

      // non-pending jobs
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(1L, connectionUuid.toString(), JobStatus.running)
        .values(2L, connectionUuid.toString(), JobStatus.failed)
        .execute()

      val result = db!!.numberOfPendingJobsByDataplaneGroupName()
      Assertions.assertEquals(result[AUTO_REGION], 0)
      Assertions.assertEquals(result[EU_REGION], 0)
    }
  }

  @Nested
  internal inner class OldestPendingJob {
    @Test
    @Throws(SQLException::class)
    fun shouldReturnOnlyPendingSeconds() {
      val expAgeSecs = 1000
      val oldestCreateAt = OffsetDateTime.now().minus(expAgeSecs.toLong(), ChronoUnit.SECONDS)
      val connectionUuid = UUID.randomUUID()
      val workspaceId = UUID.randomUUID()
      val actorDefinitionId = UUID.randomUUID()
      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
        ).values(connectionUuid, NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .execute()

      ctx!!
        .insertInto(
          Tables.WORKSPACE,
          Tables.WORKSPACE.ID,
          Tables.WORKSPACE.CUSTOMER_ID,
          Tables.WORKSPACE.NAME,
          Tables.WORKSPACE.SLUG,
          Tables.WORKSPACE.EMAIL,
          Tables.WORKSPACE.INITIAL_SETUP_COMPLETE,
          Tables.WORKSPACE.ANONYMOUS_DATA_COLLECTION,
          Tables.WORKSPACE.SEND_NEWSLETTER,
          Tables.WORKSPACE.SEND_SECURITY_UPDATES,
          Tables.WORKSPACE.DISPLAY_SETUP_WIZARD,
          Tables.WORKSPACE.TOMBSTONE,
          Tables.WORKSPACE.NOTIFICATIONS,
          Tables.WORKSPACE.FIRST_SYNC_COMPLETE,
          Tables.WORKSPACE.FEEDBACK_COMPLETE,
          Tables.WORKSPACE.CREATED_AT,
          Tables.WORKSPACE.UPDATED_AT,
          Tables.WORKSPACE.WEBHOOK_OPERATION_CONFIGS,
          Tables.WORKSPACE.NOTIFICATION_SETTINGS,
          Tables.WORKSPACE.ORGANIZATION_ID,
          Tables.WORKSPACE.DATAPLANE_GROUP_ID,
        ).values(
          workspaceId,
          UUID.randomUUID(),
          "test",
          "test-slug",
          "test@example.com",
          true,
          false,
          false,
          false,
          false,
          false,
          JSONB.valueOf("{}"),
          false,
          false,
          OffsetDateTime.now(),
          OffsetDateTime.now(),
          JSONB.valueOf("{}"),
          JSONB.valueOf("{}"),
          DEFAULT_ORGANIZATION_ID,
          euDataplaneGroupId,
        ).execute()

      ctx!!
        .insertInto(
          Tables.ACTOR_DEFINITION,
          Tables.ACTOR_DEFINITION.ID,
          Tables.ACTOR_DEFINITION.NAME,
          Tables.ACTOR_DEFINITION.ACTOR_TYPE,
          Tables.ACTOR_DEFINITION.CREATED_AT,
          Tables.ACTOR_DEFINITION.UPDATED_AT,
        ).values(actorDefinitionId, "test-source-def", ActorType.source, OffsetDateTime.now(), OffsetDateTime.now())
        .execute()

      ctx!!
        .insertInto(
          Tables.ACTOR,
          Tables.ACTOR.ID,
          Tables.ACTOR.WORKSPACE_ID,
          Tables.ACTOR.ACTOR_DEFINITION_ID,
          Tables.ACTOR.NAME,
          Tables.ACTOR.CONFIGURATION,
          Tables.ACTOR.ACTOR_TYPE,
          Tables.ACTOR.TOMBSTONE,
          Tables.ACTOR.CREATED_AT,
          Tables.ACTOR.UPDATED_AT,
          Tables.ACTOR.RESOURCE_REQUIREMENTS,
        ).values(
          srcId,
          workspaceId,
          actorDefinitionId,
          "source",
          JSONB.valueOf("{}"),
          ActorType.source,
          false,
          OffsetDateTime.now(),
          OffsetDateTime.now(),
          JSONB.valueOf("{}"),
        ).execute()

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
        ) // oldest pending job
        .values(1L, connectionUuid.toString(), JobStatus.pending, oldestCreateAt) // second-oldest pending job
        .values(2L, connectionUuid.toString(), JobStatus.pending, OffsetDateTime.now())
        .execute()
      // non-pending jobs
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(3L, connectionUuid.toString(), JobStatus.running)
        .values(4L, connectionUuid.toString(), JobStatus.failed)
        .execute()

      val result = db!!.oldestPendingJobAgeSecsByDataplaneGroupName()[EU_REGION]
      // expected age is 1000 seconds, but allow for +/- 1 second to account for timing/rounding errors
      Assertions.assertTrue(999 < result!! && result < 1001)
    }

    @Test
    fun shouldReturnNothingIfNotApplicable() {
      val connectionUuid = UUID.randomUUID()
      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .values(UUID.randomUUID(), DEFAULT_ORGANIZATION_ID, AUTO_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
        ).values(connectionUuid, NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .execute()

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(1L, connectionUuid.toString(), JobStatus.succeeded)
        .values(2L, connectionUuid.toString(), JobStatus.running)
        .values(3L, connectionUuid.toString(), JobStatus.failed)
        .execute()

      val result = db!!.oldestPendingJobAgeSecsByDataplaneGroupName()
      Assertions.assertEquals(result[EU_REGION], 0.0)
      Assertions.assertEquals(result[AUTO_REGION], 0.0)
    }
  }

  @Nested
  internal inner class OldestRunningJob {
    @Test
    fun shouldReturnOnlyRunningSeconds() {
      val expAgeSecs = 10000
      val oldestCreateAt = OffsetDateTime.now().minus(expAgeSecs.toLong(), ChronoUnit.SECONDS)
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.JOB_ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.PROCESSING_TASK_QUEUE,
        ).values(10L, 1L, AttemptStatus.running, SYNC_QUEUE)
        .values(20L, 2L, AttemptStatus.running, SYNC_QUEUE)
        .execute()
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
        ) // oldest pending job
        .values(1L, "", JobStatus.running, oldestCreateAt) // second-oldest pending job
        .values(2L, "", JobStatus.running, OffsetDateTime.now())
        .execute()

      // non-pending jobs
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(3L, "", JobStatus.pending)
        .values(4L, "", JobStatus.failed)
        .execute()

      val result = db!!.oldestRunningJobAgeSecsByTaskQueue()
      // expected age is 1000 seconds, but allow for +/- 1 second to account for timing/rounding errors
      Assertions.assertTrue(9999 < result[SYNC_QUEUE]!! && result[SYNC_QUEUE]!! < 10001L)
      Assertions.assertEquals(result[AWS_SYNC_QUEUE], 0.0)
    }

    @Test
    fun shouldReturnNothingIfNotApplicable() {
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.JOB_ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.PROCESSING_TASK_QUEUE,
        ).values(10L, 1L, SYNC_QUEUE)
        .values(20L, 2L, SYNC_QUEUE)
        .values(30L, 3L, SYNC_QUEUE)
        .execute()
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(1L, "", JobStatus.succeeded)
        .values(2L, "", JobStatus.pending)
        .values(3L, "", JobStatus.failed)
        .execute()

      val result = db!!.oldestRunningJobAgeSecsByTaskQueue()
      Assertions.assertEquals(result[SYNC_QUEUE], 0.0)
      Assertions.assertEquals(result[AWS_SYNC_QUEUE], 0.0)
    }
  }

  @Nested
  internal inner class NumActiveConnsPerWorkspace {
    @Test
    fun shouldReturnNumConnectionsBasic() {
      val workspaceId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.WORKSPACE,
          Tables.WORKSPACE.ID,
          Tables.WORKSPACE.NAME,
          Tables.WORKSPACE.TOMBSTONE,
          Tables.WORKSPACE.ORGANIZATION_ID,
          Tables.WORKSPACE.DATAPLANE_GROUP_ID,
        ).values(workspaceId, "test-0", false, DEFAULT_ORGANIZATION_ID, UUID.randomUUID())
        .execute()

      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.ACTOR,
          Tables.ACTOR.ID,
          Tables.ACTOR.WORKSPACE_ID,
          Tables.ACTOR.ACTOR_DEFINITION_ID,
          Tables.ACTOR.NAME,
          Tables.ACTOR.CONFIGURATION,
          Tables.ACTOR.ACTOR_TYPE,
          Tables.ACTOR.TOMBSTONE,
        ).values(srcId, workspaceId, SRC_DEF_ID, SRC, JSONB.valueOf("{}"), ActorType.source, false)
        .values(dstId, workspaceId, DST_DEF_ID, DEST, JSONB.valueOf("{}"), ActorType.destination, false)
        .execute()

      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
        ).values(UUID.randomUUID(), NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .values(UUID.randomUUID(), NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .execute()

      val res = db!!.numberOfActiveConnPerWorkspace()
      Assertions.assertEquals(1, res.size)
      Assertions.assertEquals(2, res[0])
    }

    @Test
    @DisplayName("should ignore deleted connections")
    fun shouldIgnoreNonRunningConnections() {
      val workspaceId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.WORKSPACE,
          Tables.WORKSPACE.ID,
          Tables.WORKSPACE.NAME,
          Tables.WORKSPACE.TOMBSTONE,
          Tables.WORKSPACE.ORGANIZATION_ID,
          Tables.WORKSPACE.DATAPLANE_GROUP_ID,
        ).values(workspaceId, "test-0", false, DEFAULT_ORGANIZATION_ID, UUID.randomUUID())
        .execute()

      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.ACTOR,
          Tables.ACTOR.ID,
          Tables.ACTOR.WORKSPACE_ID,
          Tables.ACTOR.ACTOR_DEFINITION_ID,
          Tables.ACTOR.NAME,
          Tables.ACTOR.CONFIGURATION,
          Tables.ACTOR.ACTOR_TYPE,
          Tables.ACTOR.TOMBSTONE,
        ).values(srcId, workspaceId, SRC_DEF_ID, SRC, JSONB.valueOf("{}"), ActorType.source, false)
        .values(dstId, workspaceId, DST_DEF_ID, DEST, JSONB.valueOf("{}"), ActorType.destination, false)
        .execute()

      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
        ).values(UUID.randomUUID(), NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .values(UUID.randomUUID(), NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .values(UUID.randomUUID(), NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.deprecated)
        .values(UUID.randomUUID(), NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.inactive)
        .execute()

      val res = db!!.numberOfActiveConnPerWorkspace()
      Assertions.assertEquals(1, res.size)
      Assertions.assertEquals(2, res[0])
    }

    @Test
    @DisplayName("should ignore deleted connections")
    fun shouldIgnoreDeletedWorkspaces() {
      val workspaceId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.WORKSPACE,
          Tables.WORKSPACE.ID,
          Tables.WORKSPACE.NAME,
          Tables.WORKSPACE.TOMBSTONE,
          Tables.WORKSPACE.ORGANIZATION_ID,
          Tables.WORKSPACE.DATAPLANE_GROUP_ID,
        ).values(workspaceId, "test-0", true, DEFAULT_ORGANIZATION_ID, UUID.randomUUID())
        .execute()

      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.ACTOR,
          Tables.ACTOR.ID,
          Tables.ACTOR.WORKSPACE_ID,
          Tables.ACTOR.ACTOR_DEFINITION_ID,
          Tables.ACTOR.NAME,
          Tables.ACTOR.CONFIGURATION,
          Tables.ACTOR.ACTOR_TYPE,
          Tables.ACTOR.TOMBSTONE,
        ).values(srcId, workspaceId, SRC_DEF_ID, SRC, JSONB.valueOf("{}"), ActorType.source, false)
        .values(dstId, workspaceId, DST_DEF_ID, DEST, JSONB.valueOf("{}"), ActorType.destination, false)
        .execute()

      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
        ).values(UUID.randomUUID(), NamespaceDefinitionType.source, srcId, dstId, CONN, JSONB.valueOf("{}"), true, StatusType.active)
        .execute()

      val res = db!!.numberOfActiveConnPerWorkspace()
      Assertions.assertEquals(0, res.size)
    }

    @Test
    fun shouldReturnNothingIfNotApplicable() {
      val res = db!!.numberOfActiveConnPerWorkspace()
      Assertions.assertEquals(0, res.size)
    }
  }

  @Nested
  internal inner class OverallJobRuntimeForTerminalJobsInLastHour {
    @Test
    @Throws(SQLException::class)
    fun shouldIgnoreNonTerminalJobs() {
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
        ).values(1L, "", JobStatus.running)
        .values(2L, "", JobStatus.incomplete)
        .values(3L, "", JobStatus.pending)
        .execute()

      val res = db!!.overallJobRuntimeForTerminalJobsInLastHour()
      Assertions.assertEquals(0, res.size)
    }

    @Test
    fun shouldIgnoreJobsOlderThan1Hour() {
      val updateAt = OffsetDateTime.now().minus(2, ChronoUnit.HOURS)
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
        ).values(1L, "", JobStatus.succeeded, updateAt)
        .execute()

      val res = db!!.overallJobRuntimeForTerminalJobsInLastHour()
      Assertions.assertEquals(0, res.size)
    }

    @Test
    @DisplayName("should return correct duration for terminal jobs")
    fun shouldReturnTerminalJobs() {
      val updateAt = OffsetDateTime.now()
      val expAgeSecs = 10000
      val createAt = updateAt.minus(expAgeSecs.toLong(), ChronoUnit.SECONDS)

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
        ).values(1L, "", JobStatus.succeeded, createAt, updateAt)
        .execute()
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
        ).values(2L, "", JobStatus.failed, createAt, updateAt)
        .execute()
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
        ).values(3L, "", JobStatus.cancelled, createAt, updateAt)
        .execute()

      val res = db!!.overallJobRuntimeForTerminalJobsInLastHour()
      Assertions.assertEquals(3, res.size)

      val exp =
        Map.of(
          JobStatus.succeeded,
          expAgeSecs * 1.0,
          JobStatus.cancelled,
          expAgeSecs * 1.0,
          JobStatus.failed,
          expAgeSecs * 1.0,
        )
      Assertions.assertEquals(exp, res)
    }

    @Test
    fun shouldReturnTerminalJobsComplex() {
      val updateAtNow = OffsetDateTime.now()
      val expAgeSecs = 10000
      val createAt = updateAtNow.minus(expAgeSecs.toLong(), ChronoUnit.SECONDS)

      // terminal jobs in last hour
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
        ).values(1L, "", JobStatus.succeeded, createAt, updateAtNow)
        .execute()
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
        ).values(2L, "", JobStatus.failed, createAt, updateAtNow)
        .execute()

      // old terminal jobs
      val updateAtOld = OffsetDateTime.now().minus(2, ChronoUnit.HOURS)
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
        ).values(3L, "", JobStatus.cancelled, createAt, updateAtOld)
        .execute()

      // non-terminal jobs
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
        ).values(4L, "", JobStatus.running, createAt)
        .execute()

      val res = db!!.overallJobRuntimeForTerminalJobsInLastHour()
      Assertions.assertEquals(2, res.size)

      val exp =
        Map.of(
          JobStatus.succeeded,
          expAgeSecs * 1.0,
          JobStatus.failed,
          expAgeSecs * 1.0,
        )
      Assertions.assertEquals(exp, res)
    }

    @Test
    fun shouldReturnNothingIfNotApplicable() {
      val res = db!!.overallJobRuntimeForTerminalJobsInLastHour()
      Assertions.assertEquals(0, res.size)
    }
  }

  @Nested
  internal inner class AbnormalJobsInLastDay {
    @Test
    @Throws(SQLException::class)
    fun shouldCountInJobsWithMissingRun() {
      val updateAt = OffsetDateTime.now().minus(300, ChronoUnit.HOURS)
      val connectionId = UUID.randomUUID()
      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      val syncConfigType = JobConfigType.sync
      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.SCHEDULE,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
          Tables.CONNECTION.CREATED_AT,
          Tables.CONNECTION.UPDATED_AT,
        ).values(
          connectionId,
          NamespaceDefinitionType.source,
          srcId,
          dstId,
          CONN,
          JSONB.valueOf("{}"),
          JSONB.valueOf("{\"units\": 6, \"timeUnit\": \"hours\"}"),
          false,
          StatusType.active,
          updateAt,
          updateAt,
        ).execute()

      // Jobs running in prior day will not be counted
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
        ).values(
          100L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.HOURS),
          updateAt,
          syncConfigType,
        ).values(1L, connectionId.toString(), JobStatus.succeeded, OffsetDateTime.now().minus(20, ChronoUnit.HOURS), updateAt, syncConfigType)
        .values(2L, connectionId.toString(), JobStatus.succeeded, OffsetDateTime.now().minus(10, ChronoUnit.HOURS), updateAt, syncConfigType)
        .values(3L, connectionId.toString(), JobStatus.succeeded, OffsetDateTime.now().minus(5, ChronoUnit.HOURS), updateAt, syncConfigType)
        .execute()

      val totalConnectionResult = db!!.numScheduledActiveConnectionsInLastDay()
      Assertions.assertEquals(1, totalConnectionResult)

      val abnormalConnectionResult = db!!.numberOfJobsNotRunningOnScheduleInLastDay()
      Assertions.assertEquals(1, abnormalConnectionResult)
    }

    @Test
    fun shouldNotCountNormalJobsInAbnormalMetric() {
      val updateAt = OffsetDateTime.now().minus(300, ChronoUnit.HOURS)
      val inactiveConnectionId = UUID.randomUUID()
      val activeConnectionId = UUID.randomUUID()
      val srcId = UUID.randomUUID()
      val dstId = UUID.randomUUID()
      val syncConfigType = JobConfigType.sync
      val euDataplaneGroupId = UUID.randomUUID()
      ctx!!
        .insertInto(
          Tables.DATAPLANE_GROUP,
          Tables.DATAPLANE_GROUP.ID,
          Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
          Tables.DATAPLANE_GROUP.NAME,
        ).values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, EU_REGION)
        .execute()

      ctx!!
        .insertInto(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          Tables.CONNECTION.NAMESPACE_DEFINITION,
          Tables.CONNECTION.SOURCE_ID,
          Tables.CONNECTION.DESTINATION_ID,
          Tables.CONNECTION.NAME,
          Tables.CONNECTION.CATALOG,
          Tables.CONNECTION.SCHEDULE,
          Tables.CONNECTION.MANUAL,
          Tables.CONNECTION.STATUS,
          Tables.CONNECTION.CREATED_AT,
          Tables.CONNECTION.UPDATED_AT,
        ).values(
          inactiveConnectionId,
          NamespaceDefinitionType.source,
          srcId,
          dstId,
          CONN,
          JSONB.valueOf("{}"),
          JSONB.valueOf("{\"units\": 12, \"timeUnit\": \"hours\"}"),
          false,
          StatusType.inactive,
          updateAt,
          updateAt,
        ).values(
          activeConnectionId,
          NamespaceDefinitionType.source,
          srcId,
          dstId,
          CONN,
          JSONB.valueOf("{}"),
          JSONB.valueOf("{\"units\": 12, \"timeUnit\": \"hours\"}"),
          false,
          StatusType.active,
          updateAt,
          updateAt,
        ).execute()

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
        ).values(
          1L,
          activeConnectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.HOURS),
          updateAt,
          syncConfigType,
        ).values(
          2L,
          activeConnectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(10, ChronoUnit.HOURS),
          updateAt,
          syncConfigType,
        ).execute()

      val totalConnectionResult = db!!.numScheduledActiveConnectionsInLastDay()
      Assertions.assertEquals(1, totalConnectionResult)

      val abnormalConnectionResult = db!!.numberOfJobsNotRunningOnScheduleInLastDay()
      Assertions.assertEquals(0, abnormalConnectionResult)
    }
  }

  @Nested
  internal inner class UnusuallyLongJobs {
    @Test
    @Throws(SQLException::class)
    fun shouldCountJobsWithUnusuallyLongTime() {
      val connectionId = UUID.randomUUID()
      val syncConfigType = JobConfigType.sync
      val config =
        JSONB.valueOf(
          """
          {
           "sync": {
              "sourceDockerImage": "airbyte/source-postgres-1.1.0",
              "destinationDockerImage": "airbyte/destination-s3-1.4.0",
              "workspaceId": "81249e08-f71c-4743-98da-ed3c6c893132"
            }
          }
          
          """.trimIndent(),
        )

      // Current job has been running for 12 hours while the previous 5 jobs runs 2 hours. Avg will be 2
      // hours.
      // Thus latest job will be counted as an unusually long-running job.
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG,
        ).values(
          100L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(26, ChronoUnit.HOURS),
          syncConfigType,
          config,
        ).values(
          1L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          syncConfigType,
          config,
        ).values(
          2L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(16, ChronoUnit.HOURS),
          syncConfigType,
          config,
        ).values(
          3L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(16, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(14, ChronoUnit.HOURS),
          syncConfigType,
          config,
        ).values(
          4L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(14, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
          syncConfigType,
          config,
        ).values(
          5L,
          connectionId.toString(),
          JobStatus.running,
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
          syncConfigType,
          config,
        ).execute()

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.JOB_ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.UPDATED_AT,
        ).values(
          100L,
          100L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(26, ChronoUnit.HOURS),
        ).values(
          1L,
          1L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
        ).values(
          2L,
          2L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(16, ChronoUnit.HOURS),
        ).values(
          3L,
          3L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(16, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(14, ChronoUnit.HOURS),
        ).values(
          4L,
          4L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(14, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
        ).values(
          5L,
          5L,
          AttemptStatus.running,
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
        ).execute()

      val longRunningJobs = db!!.unusuallyLongRunningJobs()
      Assertions.assertEquals(1, longRunningJobs.size)
      val job = longRunningJobs[0]
      Assertions.assertEquals("airbyte/source-postgres-1.1.0", job.sourceDockerImage)
      Assertions.assertEquals("airbyte/destination-s3-1.4.0", job.destinationDockerImage)
      Assertions.assertEquals(connectionId.toString(), job.connectionId)
    }

    @Test
    @Throws(SQLException::class)
    fun handlesNullConfigRows() {
      val connectionId = UUID.randomUUID()
      val syncConfigType = JobConfigType.sync

      // same as above but no value passed for `config`
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
        ).values(
          100L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(26, ChronoUnit.HOURS),
          syncConfigType,
        ).values(
          1L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          syncConfigType,
        ).values(
          2L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(16, ChronoUnit.HOURS),
          syncConfigType,
        ).values(
          3L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(16, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(14, ChronoUnit.HOURS),
          syncConfigType,
        ).values(
          4L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(14, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
          syncConfigType,
        ).values(
          5L,
          connectionId.toString(),
          JobStatus.running,
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
          syncConfigType,
        ).execute()

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.JOB_ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.UPDATED_AT,
        ).values(
          100L,
          100L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(26, ChronoUnit.HOURS),
        ).values(
          1L,
          1L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
        ).values(
          2L,
          2L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(16, ChronoUnit.HOURS),
        ).values(
          3L,
          3L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(16, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(14, ChronoUnit.HOURS),
        ).values(
          4L,
          4L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(14, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
        ).values(
          5L,
          5L,
          AttemptStatus.running,
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(12, ChronoUnit.HOURS),
        ).execute()

      val longRunningJobs = db!!.unusuallyLongRunningJobs()
      Assertions.assertEquals(1, longRunningJobs.size)
    }

    @Test
    @Throws(SQLException::class)
    fun shouldNotCountInJobsWithinFifteenMinutes() {
      val connectionId = UUID.randomUUID()
      val syncConfigType = JobConfigType.sync

      // Latest job runs 14 minutes while the previous 5 jobs runs average about 3 minutes.
      // Despite it has been more than 2x than avg it's still within 15 minutes threshold, thus this
      // shouldn't be
      // counted in.
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
        ).values(
          100L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(26, ChronoUnit.MINUTES),
          syncConfigType,
        ).values(
          1L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(18, ChronoUnit.MINUTES),
          syncConfigType,
        ).values(
          2L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(18, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(16, ChronoUnit.MINUTES),
          syncConfigType,
        ).values(
          3L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(16, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(14, ChronoUnit.MINUTES),
          syncConfigType,
        ).values(
          4L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(14, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(2, ChronoUnit.MINUTES),
          syncConfigType,
        ).values(
          5L,
          connectionId.toString(),
          JobStatus.running,
          OffsetDateTime.now().minus(14, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(2, ChronoUnit.MINUTES),
          syncConfigType,
        ).execute()

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.JOB_ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.UPDATED_AT,
        ).values(
          100L,
          100L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(26, ChronoUnit.MINUTES),
        ).values(
          1L,
          1L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(18, ChronoUnit.MINUTES),
        ).values(
          2L,
          2L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(18, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(16, ChronoUnit.MINUTES),
        ).values(
          3L,
          3L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(26, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(14, ChronoUnit.MINUTES),
        ).values(
          4L,
          4L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(18, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(17, ChronoUnit.MINUTES),
        ).values(
          5L,
          5L,
          AttemptStatus.running,
          OffsetDateTime.now().minus(14, ChronoUnit.MINUTES),
          OffsetDateTime.now().minus(14, ChronoUnit.MINUTES),
        ).execute()

      val numOfUnusuallyLongRunningJobs = db!!.unusuallyLongRunningJobs().size
      Assertions.assertEquals(0, numOfUnusuallyLongRunningJobs)
    }

    @Test
    @Throws(SQLException::class)
    fun shouldSkipInsufficientJobRuns() {
      val connectionId = UUID.randomUUID()
      val syncConfigType = JobConfigType.sync

      // Require at least 5 runs in last week to get meaningful average runtime.
      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
        ).values(
          100L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(26, ChronoUnit.HOURS),
          syncConfigType,
        ).values(
          1L,
          connectionId.toString(),
          JobStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          syncConfigType,
        ).values(
          2L,
          connectionId.toString(),
          JobStatus.running,
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(1, ChronoUnit.HOURS),
          syncConfigType,
        ).execute()

      ctx!!
        .insertInto(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.JOB_ID,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.STATUS,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.CREATED_AT,
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.UPDATED_AT,
        ).values(
          100L,
          100L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(28, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(26, ChronoUnit.HOURS),
        ).values(
          1L,
          1L,
          AttemptStatus.succeeded,
          OffsetDateTime.now().minus(20, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
        ).values(
          2L,
          2L,
          AttemptStatus.running,
          OffsetDateTime.now().minus(18, ChronoUnit.HOURS),
          OffsetDateTime.now().minus(1, ChronoUnit.HOURS),
        ).execute()

      val numOfUnusuallyLongRunningJobs = db!!.unusuallyLongRunningJobs().size
      Assertions.assertEquals(0, numOfUnusuallyLongRunningJobs)
    }
  }

  companion object {
    private const val SRC = "src"
    private const val DEST = "dst"
    private const val CONN = "conn"
    private const val SYNC_QUEUE = "SYNC"
    private const val AWS_SYNC_QUEUE = "AWS_PARIS_SYNC"
    private const val AUTO_REGION = "AUTO"
    private const val EU_REGION = "EU"

    val SRC_DEF_ID: UUID = UUID.randomUUID()
    val DST_DEF_ID: UUID = UUID.randomUUID()
    val SRC_DEF_VER_ID: UUID = UUID.randomUUID()
    val DST_DEF_VER_ID: UUID = UUID.randomUUID()
    internal var db: MetricRepository? = null
    internal var ctx: DSLContext? = null
  }
}
