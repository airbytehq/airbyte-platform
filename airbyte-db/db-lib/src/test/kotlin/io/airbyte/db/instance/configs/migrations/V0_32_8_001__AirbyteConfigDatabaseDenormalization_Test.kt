/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Notification
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.Schedule
import io.airbyte.config.SourceConnection
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardSyncState
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.State
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.Companion.actorDefinitionDoesNotExist
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.Companion.actorDoesNotExist
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.Companion.connectionDoesNotExist
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.Companion.operationDoesNotExist
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.Companion.workspaceDoesNotExist
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V0_32_8_001__AirbyteConfigDatabaseDenormalization_Test : AbstractConfigsDatabaseTest() {
  @Test
  fun testCompleteMigration() {
    val context = dslContext!!
    SetupForNormalizedTablesTest.setup(context)

    V0_32_8_001__AirbyteConfigDatabaseDenormalization.migrate(context)

    assertDataForWorkspace(context)
    assertDataForSourceDefinition(context)
    assertDataForDestinationDefinition(context)
    assertDataForSourceConnection(context)
    assertDataForDestinationConnection(context)
    assertDataForSourceOauthParams(context)
    assertDataForDestinationOauthParams(context)
    assertDataForOperations(context)
    assertDataForConnections(context)
    assertDataForStandardSyncStates(context)
  }

  private fun assertDataForWorkspace(context: DSLContext) {
    val workspaces =
      context
        .select(DSL.asterisk())
        .from(DSL.table("workspace"))
        .fetch()
    Assertions.assertEquals(1, workspaces.size)

    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
    val slug = DSL.field("slug", SQLDataType.VARCHAR(256).nullable(false))
    val initialSetupComplete = DSL.field("initial_setup_complete", SQLDataType.BOOLEAN.nullable(false))
    val customerId = DSL.field("customer_id", SQLDataType.UUID.nullable(true))
    val email = DSL.field("email", SQLDataType.VARCHAR(256).nullable(true))
    val anonymousDataCollection = DSL.field("anonymous_data_collection", SQLDataType.BOOLEAN.nullable(true))
    val sendNewsletter = DSL.field("send_newsletter", SQLDataType.BOOLEAN.nullable(true))
    val sendSecurityUpdates = DSL.field("send_security_updates", SQLDataType.BOOLEAN.nullable(true))
    val displaySetupWizard = DSL.field("display_setup_wizard", SQLDataType.BOOLEAN.nullable(true))
    val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(true))
    val notifications = DSL.field("notifications", SQLDataType.JSONB.nullable(true))
    val firstSyncComplete = DSL.field("first_sync_complete", SQLDataType.BOOLEAN.nullable(true))
    val feedbackComplete = DSL.field("feedback_complete", SQLDataType.BOOLEAN.nullable(true))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val workspace = workspaces[0]

    val notificationList: MutableList<Notification> = ArrayList()
    val fetchedNotifications: List<*> = Jsons.deserialize(workspace.get(notifications).data(), List::class.java)
    for (notification in fetchedNotifications) {
      notificationList.add(Jsons.convertValue(notification, Notification::class.java))
    }
    val workspaceFromNewTable =
      StandardWorkspace()
        .withWorkspaceId(workspace.get(id))
        .withName(workspace.get(name))
        .withSlug(workspace.get(slug))
        .withInitialSetupComplete(workspace.get(initialSetupComplete))
        .withCustomerId(workspace.get(customerId))
        .withEmail(workspace.get(email))
        .withAnonymousDataCollection(workspace.get(anonymousDataCollection))
        .withNews(workspace.get(sendNewsletter))
        .withSecurityUpdates(workspace.get(sendSecurityUpdates))
        .withDisplaySetupWizard(workspace.get(displaySetupWizard))
        .withTombstone(workspace.get(tombstone))
        .withNotifications(notificationList)
        .withFirstCompletedSync(workspace.get(firstSyncComplete))
        .withFeedbackDone(workspace.get(feedbackComplete))
    Assertions.assertEquals(SetupForNormalizedTablesTest.standardWorkspace(), workspaceFromNewTable)
    Assertions.assertEquals(SetupForNormalizedTablesTest.now(), workspace.get(createdAt).toInstant())
    Assertions.assertEquals(SetupForNormalizedTablesTest.now(), workspace.get(updatedAt).toInstant())
    Assertions.assertFalse(
      workspaceDoesNotExist(
        SetupForNormalizedTablesTest.standardWorkspace().workspaceId,
        context,
      ),
    )
    Assertions.assertTrue(workspaceDoesNotExist(UUID.randomUUID(), context))
  }

  private fun assertDataForSourceDefinition(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
    val dockerRepository = DSL.field("docker_repository", SQLDataType.VARCHAR(256).nullable(false))
    val dockerImageTag = DSL.field("docker_image_tag", SQLDataType.VARCHAR(256).nullable(false))
    val documentationUrl = DSL.field("documentation_url", SQLDataType.VARCHAR(256).nullable(false))
    val spec = DSL.field("spec", SQLDataType.JSONB.nullable(false))
    val icon = DSL.field("icon", SQLDataType.VARCHAR(256).nullable(true))
    val actorType =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java,
          ).nullable(false),
      )
    val sourceType =
      DSL.field(
        "source_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.SourceType::class.java,
          ).nullable(true),
      )
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val sourceDefinitions =
      context
        .select(DSL.asterisk())
        .from(DSL.table("actor_definition"))
        .where(actorType.eq(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source))
        .fetch()
    val expectedDefinitions = SetupForNormalizedTablesTest.standardSourceDefinitions()
    Assertions.assertEquals(expectedDefinitions.size, sourceDefinitions.size)
    Assertions.assertTrue(actorDefinitionDoesNotExist(UUID.randomUUID(), context))
    for (sourceDefinition in sourceDefinitions) {
      val standardSourceDefinition =
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.StandardSourceDefinition(
          sourceDefinition.get(id),
          sourceDefinition.get(name),
          sourceDefinition.get(dockerRepository),
          sourceDefinition.get(dockerImageTag),
          sourceDefinition.get(documentationUrl),
          sourceDefinition.get(icon),
          sourceDefinition
            .get(
              sourceType,
              String::class.java,
            ).toEnum<V0_32_8_001__AirbyteConfigDatabaseDenormalization.SourceType>()!!,
          Jsons.deserialize(
            sourceDefinition.get(spec).data(),
            ConnectorSpecification::class.java,
          ),
        )
      Assertions.assertTrue(expectedDefinitions.contains(standardSourceDefinition))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), sourceDefinition.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), sourceDefinition.get(updatedAt).toInstant())
      Assertions.assertFalse(
        actorDefinitionDoesNotExist(standardSourceDefinition.sourceDefinitionId, context),
      )
    }
  }

  private fun assertDataForDestinationDefinition(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
    val dockerRepository = DSL.field("docker_repository", SQLDataType.VARCHAR(256).nullable(false))
    val dockerImageTag = DSL.field("docker_image_tag", SQLDataType.VARCHAR(256).nullable(false))
    val documentationUrl = DSL.field("documentation_url", SQLDataType.VARCHAR(256).nullable(false))
    val spec = DSL.field("spec", SQLDataType.JSONB.nullable(false))
    val icon = DSL.field("icon", SQLDataType.VARCHAR(256).nullable(true))
    val actorType =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java,
          ).nullable(false),
      )
    val sourceType =
      DSL.field(
        "source_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.SourceType::class.java,
          ).nullable(true),
      )
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val destinationDefinitions =
      context
        .select(DSL.asterisk())
        .from(DSL.table("actor_definition"))
        .where(actorType.eq(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.destination))
        .fetch()
    val expectedDefinitions = SetupForNormalizedTablesTest.standardDestinationDefinitions()
    Assertions.assertEquals(expectedDefinitions.size, destinationDefinitions.size)
    Assertions.assertTrue(actorDefinitionDoesNotExist(UUID.randomUUID(), context))
    for (record in destinationDefinitions) {
      val standardDestinationDefinition =
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.StandardDestinationDefinition(
          record.get(id),
          record.get(name),
          record.get(dockerRepository),
          record.get(dockerImageTag),
          record.get(documentationUrl),
          record.get(icon),
          Jsons.deserialize(
            record.get(spec).data(),
            ConnectorSpecification::class.java,
          ),
        )
      Assertions.assertTrue(expectedDefinitions.contains(standardDestinationDefinition))
      Assertions.assertNull(record.get(sourceType))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
      Assertions.assertFalse(
        actorDefinitionDoesNotExist(
          standardDestinationDefinition.destinationDefinitionId,
          context,
        ),
      )
    }
  }

  private fun assertDataForSourceConnection(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
    val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
    val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
    val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
    val actorType =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java,
          ).nullable(false),
      )
    val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(false))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val sourceConnections =
      context
        .select(DSL.asterisk())
        .from(DSL.table("actor"))
        .where(actorType.eq(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source))
        .fetch()
    val expectedDefinitions = SetupForNormalizedTablesTest.sourceConnections()
    Assertions.assertEquals(expectedDefinitions.size, sourceConnections.size)
    Assertions.assertTrue(actorDoesNotExist(UUID.randomUUID(), context))
    for (record in sourceConnections) {
      val sourceConnection =
        SourceConnection()
          .withSourceId(record.get(id))
          .withConfiguration(Jsons.deserialize(record.get(configuration).data()))
          .withWorkspaceId(record.get(workspaceId))
          .withSourceDefinitionId(record.get(actorDefinitionId))
          .withTombstone(record.get(tombstone))
          .withName(record.get(name))

      Assertions.assertTrue(expectedDefinitions.contains(sourceConnection))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
      Assertions.assertFalse(actorDoesNotExist(sourceConnection.sourceId, context))
    }
  }

  private fun assertDataForDestinationConnection(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
    val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
    val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
    val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
    val actorType =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java,
          ).nullable(false),
      )
    val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(false))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val destinationConnections =
      context
        .select(DSL.asterisk())
        .from(DSL.table("actor"))
        .where(actorType.eq(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.destination))
        .fetch()
    val expectedDefinitions = SetupForNormalizedTablesTest.destinationConnections()
    Assertions.assertEquals(expectedDefinitions.size, destinationConnections.size)
    Assertions.assertTrue(actorDoesNotExist(UUID.randomUUID(), context))
    for (record in destinationConnections) {
      val destinationConnection =
        DestinationConnection()
          .withDestinationId(record.get(id))
          .withConfiguration(Jsons.deserialize(record.get(configuration).data()))
          .withWorkspaceId(record.get(workspaceId))
          .withDestinationDefinitionId(record.get(actorDefinitionId))
          .withTombstone(record.get(tombstone))
          .withName(record.get(name))

      Assertions.assertTrue(expectedDefinitions.contains(destinationConnection))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
      Assertions.assertFalse(actorDoesNotExist(destinationConnection.destinationId, context))
    }
  }

  private fun assertDataForSourceOauthParams(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
    val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
    val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
    val actorType =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java,
          ).nullable(false),
      )
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val sourceOauthParams =
      context
        .select(DSL.asterisk())
        .from(DSL.table("actor_oauth_parameter"))
        .where(actorType.eq(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source))
        .fetch()
    val expectedDefinitions =
      SetupForNormalizedTablesTest
        .sourceOauthParameters()
        .stream()
        .filter { c: SourceOAuthParameter? -> c!!.workspaceId != null }
        .toList()
    Assertions.assertEquals(expectedDefinitions.size, sourceOauthParams.size)

    for (record in sourceOauthParams) {
      val sourceOAuthParameter =
        SourceOAuthParameter()
          .withOauthParameterId(record.get(id))
          .withConfiguration(Jsons.deserialize(record.get(configuration).data()))
          .withWorkspaceId(record.get(workspaceId))
          .withSourceDefinitionId(record.get(actorDefinitionId))
      Assertions.assertTrue(expectedDefinitions.contains(sourceOAuthParameter))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
    }
  }

  private fun assertDataForDestinationOauthParams(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
    val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
    val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
    val actorType =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java,
          ).nullable(false),
      )
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val destinationOauthParams =
      context
        .select(DSL.asterisk())
        .from(DSL.table("actor_oauth_parameter"))
        .where(actorType.eq(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.destination))
        .fetch()
    val expectedDefinitions =
      SetupForNormalizedTablesTest
        .destinationOauthParameters()
        .stream()
        .filter { c: DestinationOAuthParameter? -> c!!.workspaceId != null }
        .toList()
    Assertions.assertEquals(expectedDefinitions.size, destinationOauthParams.size)

    for (record in destinationOauthParams) {
      val destinationOAuthParameter =
        DestinationOAuthParameter()
          .withOauthParameterId(record.get(id))
          .withConfiguration(Jsons.deserialize(record.get(configuration).data()))
          .withWorkspaceId(record.get(workspaceId))
          .withDestinationDefinitionId(record.get(actorDefinitionId))
      Assertions.assertTrue(expectedDefinitions.contains(destinationOAuthParameter))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
    }
  }

  private fun assertDataForOperations(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
    val operatorType =
      DSL.field(
        "operator_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.OperatorType::class.java,
          ).nullable(false),
      )
    val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(true))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val standardSyncOperations =
      context
        .select(DSL.asterisk())
        .from(DSL.table("operation"))
        .fetch()
    val expectedDefinitions = SetupForNormalizedTablesTest.standardSyncOperations()
    Assertions.assertEquals(expectedDefinitions.size, standardSyncOperations.size)
    Assertions.assertTrue(operationDoesNotExist(UUID.randomUUID(), context))
    for (record in standardSyncOperations) {
      val standardSyncOperation =
        StandardSyncOperation()
          .withOperationId(record.get(id))
          .withName(record.get(name))
          .withWorkspaceId(record.get(workspaceId))
          .withOperatorType(
            record
              .get(
                operatorType,
                String::class.java,
              ).toEnum<StandardSyncOperation.OperatorType>()
              ?: StandardSyncOperation.OperatorType.WEBHOOK,
          ).withOperatorWebhook(OperatorWebhook())
          .withTombstone(record.get(tombstone))

      Assertions.assertTrue(expectedDefinitions.contains(standardSyncOperation))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
      Assertions
        .assertFalse(operationDoesNotExist(standardSyncOperation.operationId, context))
    }
  }

  private fun assertDataForConnections(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val namespaceDefinition =
      DSL
        .field(
          "namespace_definition",
          SQLDataType.VARCHAR
            .asEnumDataType(
              V0_32_8_001__AirbyteConfigDatabaseDenormalization.NamespaceDefinitionType::class.java,
            ).nullable(false),
        )
    val namespaceFormat = DSL.field("namespace_format", SQLDataType.VARCHAR(256).nullable(true))
    val prefix = DSL.field("prefix", SQLDataType.VARCHAR(256).nullable(true))
    val sourceId = DSL.field("source_id", SQLDataType.UUID.nullable(false))
    val destinationId = DSL.field("destination_id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
    val catalog = DSL.field("catalog", SQLDataType.JSONB.nullable(false))
    val status =
      DSL.field(
        "status",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.StatusType::class.java,
          ).nullable(true),
      )
    val schedule = DSL.field("schedule", SQLDataType.JSONB.nullable(true))
    val manual = DSL.field("manual", SQLDataType.BOOLEAN.nullable(false))
    val resourceRequirements = DSL.field("resource_requirements", SQLDataType.JSONB.nullable(true))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val standardSyncs =
      context
        .select(DSL.asterisk())
        .from(DSL.table("connection"))
        .fetch()
    val expectedStandardSyncs = SetupForNormalizedTablesTest.standardSyncs()
    Assertions.assertEquals(expectedStandardSyncs.size, standardSyncs.size)
    Assertions.assertTrue(connectionDoesNotExist(UUID.randomUUID(), context))
    for (record in standardSyncs) {
      val standardSync =
        StandardSync()
          .withConnectionId(record.get(id))
          .withNamespaceDefinition(
            record
              .get(
                namespaceDefinition,
                String::class.java,
              ).toEnum<JobSyncConfig.NamespaceDefinitionType>()!!,
          ).withNamespaceFormat(record.get(namespaceFormat))
          .withPrefix(record.get(prefix))
          .withSourceId(record.get(sourceId))
          .withDestinationId(record.get(destinationId))
          .withName(record.get(name))
          .withCatalog(
            Jsons.deserialize(
              record.get(catalog).data(),
              ConfiguredAirbyteCatalog::class.java,
            ),
          ).withStatus(
            record
              .get(
                status,
                String::class.java,
              ).toEnum<StandardSync.Status>()!!,
          ).withSchedule(
            Jsons.deserialize(
              record.get(schedule).data(),
              Schedule::class.java,
            ),
          ).withManual(record.get(manual))
          .withOperationIds(connectionOperationIds(record.get(id), context))
          .withResourceRequirements(
            Jsons.deserialize(
              record.get(resourceRequirements).data(),
              ResourceRequirements::class.java,
            ),
          )

      Assertions.assertTrue(expectedStandardSyncs.contains(standardSync))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
      Assertions.assertFalse(connectionDoesNotExist(standardSync.connectionId, context))
    }
  }

  private fun connectionOperationIds(
    connectionIdTo: UUID,
    context: DSLContext,
  ): List<UUID> {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
    val operationId = DSL.field("operation_id", SQLDataType.UUID.nullable(false))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val connectionOperations =
      context
        .select(DSL.asterisk())
        .from(DSL.table("connection_operation"))
        .where(connectionId.eq(connectionIdTo))
        .fetch()

    val ids: MutableList<UUID> = ArrayList()

    for (record in connectionOperations) {
      ids.add(record.get(operationId))
      Assertions.assertNotNull(record.get(id))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
    }

    return ids
  }

  private fun assertDataForStandardSyncStates(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
    val state = DSL.field("state", SQLDataType.JSONB.nullable(true))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val standardSyncStates =
      context
        .select(DSL.asterisk())
        .from(DSL.table("state"))
        .fetch()
    val expectedStandardSyncsStates = SetupForNormalizedTablesTest.standardSyncStates()
    Assertions.assertEquals(expectedStandardSyncsStates.size, standardSyncStates.size)

    for (record in standardSyncStates) {
      val standardSyncState =
        StandardSyncState()
          .withConnectionId(record.get(connectionId))
          .withState(
            Jsons.deserialize(
              record.get(state).data(),
              State::class.java,
            ),
          )

      Assertions.assertTrue(expectedStandardSyncsStates.contains(standardSyncState))
      Assertions.assertNotNull(record.get(id))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
    }
  }
}
