/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.fasterxml.jackson.core.type.TypeReference
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorCatalog.CatalogType
import io.airbyte.config.ActorCatalogFetchEvent
import io.airbyte.config.ActorCatalogWithUpdatedAt
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AllowedHosts
import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.ConnectorRegistryEntryMetrics
import io.airbyte.config.DeclarativeManifest
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.FieldSelectionData
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Notification
import io.airbyte.config.NotificationSettings
import io.airbyte.config.Organization
import io.airbyte.config.ReleaseStage
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.ScopeType
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.SecretPersistenceCoordinate
import io.airbyte.config.SourceConnection
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SuggestedStreams
import io.airbyte.config.SupportLevel
import io.airbyte.config.Tag
import io.airbyte.config.WorkspaceServiceAccount
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.BackfillPreference
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.TagRecord
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.jooq.Record
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Date
import java.util.Optional
import java.util.UUID

/**
 * Provides static methods for converting from repository layer results (often in the form of a jooq
 * [Record]) to config models.
 */
object DbConverter {
  /**
   * Build connection (a.k.a. StandardSync) from db record.
   *
   * @param record db record.
   * @param connectionOperationId connection operation id.
   * @return connection (a.k.a. StandardSync)
   */
  fun buildStandardSync(
    record: Record,
    connectionOperationId: List<UUID?>?,
    notificationConfigurations: List<NotificationConfigurationRecord>,
    tagRecords: List<TagRecord>,
  ): StandardSync {
    val isWebhookNotificationEnabled =
      notificationConfigurations
        .stream()
        .filter { notificationConfiguration: NotificationConfigurationRecord ->
          notificationConfiguration
            .notificationType == NotificationType.webhook &&
            notificationConfiguration.enabled
        }.findAny()
        .isPresent

    val isEmailNotificationEnabled =
      notificationConfigurations
        .stream()
        .filter { notificationConfiguration: NotificationConfigurationRecord ->
          notificationConfiguration
            .notificationType == NotificationType.email &&
            notificationConfiguration.enabled
        }.findAny()
        .isPresent

    val tags: MutableList<Tag> = ArrayList()
    for (tagRecord in tagRecords) {
      tags.add(
        Tag()
          .withTagId(tagRecord.id)
          .withWorkspaceId(tagRecord.workspaceId)
          .withName(tagRecord.name)
          .withColor(tagRecord.color),
      )
    }

    return StandardSync()
      .withConnectionId(record.get(Tables.CONNECTION.ID))
      .withNamespaceDefinition(
        record
          .get(
            Tables.CONNECTION.NAMESPACE_DEFINITION,
            String::class.java,
          ).toEnum<JobSyncConfig.NamespaceDefinitionType>()!!,
      ).withNamespaceFormat(record.get(Tables.CONNECTION.NAMESPACE_FORMAT))
      .withPrefix(record.get(Tables.CONNECTION.PREFIX))
      .withSourceId(record.get(Tables.CONNECTION.SOURCE_ID))
      .withDestinationId(record.get(Tables.CONNECTION.DESTINATION_ID))
      .withName(record.get(Tables.CONNECTION.NAME))
      .withCatalog(parseConfiguredAirbyteCatalog(record.get(Tables.CONNECTION.CATALOG).data()))
      .withFieldSelectionData(
        if (record.get(Tables.CONNECTION.FIELD_SELECTION_DATA) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.CONNECTION.FIELD_SELECTION_DATA).data(),
            FieldSelectionData::class.java,
          )
        },
      ).withStatus(
        if (record.get(Tables.CONNECTION.STATUS) == null) {
          null
        } else {
          record
            .get(
              Tables.CONNECTION.STATUS,
              String::class.java,
            ).toEnum<StandardSync.Status>()!!
        },
      ).withSchedule(
        Jsons.deserialize(
          record.get(Tables.CONNECTION.SCHEDULE).data(),
          Schedule::class.java,
        ),
      ).withManual(record.get(Tables.CONNECTION.MANUAL))
      .withScheduleType(
        if (record.get(Tables.CONNECTION.SCHEDULE_TYPE) == null) {
          null
        } else {
          record
            .get(
              Tables.CONNECTION.SCHEDULE_TYPE,
              String::class.java,
            ).toEnum<StandardSync.ScheduleType>()!!
        },
      ).withScheduleData(
        if (record.get(Tables.CONNECTION.SCHEDULE_DATA) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.CONNECTION.SCHEDULE_DATA).data(),
            ScheduleData::class.java,
          )
        },
      ).withOperationIds(connectionOperationId)
      .withResourceRequirements(
        Jsons.deserialize(
          record.get(Tables.CONNECTION.RESOURCE_REQUIREMENTS).data(),
          ResourceRequirements::class.java,
        ),
      ).withSourceCatalogId(record.get(Tables.CONNECTION.SOURCE_CATALOG_ID))
      .withDestinationCatalogId(record.get(Tables.CONNECTION.DESTINATION_CATALOG_ID))
      .withBreakingChange(record.get(Tables.CONNECTION.BREAKING_CHANGE))
      .withNonBreakingChangesPreference(
        Optional
          .ofNullable(record.get(Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS))
          .orElse(AutoPropagationStatus.ignore)
          .literal
          .toEnum<StandardSync.NonBreakingChangesPreference>()!!,
      ).withNotifySchemaChanges(isWebhookNotificationEnabled)
      .withNotifySchemaChangesByEmail(isEmailNotificationEnabled)
      .withCreatedAt(record.get(Tables.CONNECTION.CREATED_AT, OffsetDateTime::class.java).toEpochSecond())
      .withBackfillPreference(
        Optional
          .ofNullable(record.get(Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
          .orElse(BackfillPreference.disabled)
          .literal
          .toEnum<StandardSync.BackfillPreference>()!!,
      ).withTags(tags)
  }

  private fun parseConfiguredAirbyteCatalog(configuredAirbyteCatalogString: String): ConfiguredAirbyteCatalog =
    Jsons.deserialize(configuredAirbyteCatalogString, ConfiguredAirbyteCatalog::class.java)

  /**
   * Build workspace from db record.
   *
   * @param record db record
   * @return workspace
   */
  @JvmStatic
  fun buildStandardWorkspace(record: Record): StandardWorkspace {
    val notificationList: MutableList<Notification> = ArrayList()
    val fetchedNotifications: List<*> =
      Jsons.deserialize(
        record.get(Tables.WORKSPACE.NOTIFICATIONS).data(),
        MutableList::class.java,
      )
    for (notification in fetchedNotifications) {
      notificationList.add(Jsons.convertValue(notification, Notification::class.java))
    }
    return StandardWorkspace()
      .withWorkspaceId(record.get(Tables.WORKSPACE.ID))
      .withName(record.get(Tables.WORKSPACE.NAME))
      .withSlug(record.get(Tables.WORKSPACE.SLUG))
      .withInitialSetupComplete(record.get(Tables.WORKSPACE.INITIAL_SETUP_COMPLETE))
      .withCustomerId(record.get(Tables.WORKSPACE.CUSTOMER_ID))
      .withEmail(record.get(Tables.WORKSPACE.EMAIL))
      .withAnonymousDataCollection(record.get(Tables.WORKSPACE.ANONYMOUS_DATA_COLLECTION))
      .withNews(record.get(Tables.WORKSPACE.SEND_NEWSLETTER))
      .withSecurityUpdates(record.get(Tables.WORKSPACE.SEND_SECURITY_UPDATES))
      .withDisplaySetupWizard(record.get(Tables.WORKSPACE.DISPLAY_SETUP_WIZARD))
      .withTombstone(record.get(Tables.WORKSPACE.TOMBSTONE))
      .withNotifications(notificationList)
      .withNotificationSettings(
        if (record.get(Tables.WORKSPACE.NOTIFICATION_SETTINGS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.WORKSPACE.NOTIFICATION_SETTINGS).data(),
            NotificationSettings::class.java,
          )
        },
      ).withFirstCompletedSync(record.get(Tables.WORKSPACE.FIRST_SYNC_COMPLETE))
      .withFeedbackDone(record.get(Tables.WORKSPACE.FEEDBACK_COMPLETE))
      .withDataplaneGroupId(record.get(Tables.WORKSPACE.DATAPLANE_GROUP_ID))
      .withWebhookOperationConfigs(
        if (record.get(Tables.WORKSPACE.WEBHOOK_OPERATION_CONFIGS) == null) {
          null
        } else {
          Jsons.deserialize(record.get(Tables.WORKSPACE.WEBHOOK_OPERATION_CONFIGS).data())
        },
      ).withOrganizationId(record.get(Tables.WORKSPACE.ORGANIZATION_ID))
      .withCreatedAt(record.get(Tables.WORKSPACE.CREATED_AT, OffsetDateTime::class.java).toEpochSecond())
      .withUpdatedAt(record.get(Tables.WORKSPACE.UPDATED_AT, OffsetDateTime::class.java).toEpochSecond())
  }

  /**
   * Build organization from db record.
   *
   * @param record db record
   * @return organization
   */
  fun buildOrganization(record: Record): Organization =
    Organization()
      .withOrganizationId(record.get(Tables.ORGANIZATION.ID))
      .withName(record.get(Tables.ORGANIZATION.NAME))
      .withUserId(record.get(Tables.ORGANIZATION.USER_ID))
      .withEmail(record.get(Tables.ORGANIZATION.EMAIL))

  /**
   * Build source from db record.
   *
   * @param record db record
   * @return source
   */
  fun buildSourceConnection(record: Record): SourceConnection =
    SourceConnection()
      .withSourceId(record.get(Tables.ACTOR.ID))
      .withConfiguration(Jsons.deserialize(record.get(Tables.ACTOR.CONFIGURATION).data()))
      .withWorkspaceId(record.get(Tables.ACTOR.WORKSPACE_ID))
      .withSourceDefinitionId(record.get(Tables.ACTOR.ACTOR_DEFINITION_ID))
      .withTombstone(record.get(Tables.ACTOR.TOMBSTONE))
      .withName(record.get(Tables.ACTOR.NAME))
      .withCreatedAt(record.get(Tables.ACTOR.CREATED_AT).toEpochSecond())
      .withUpdatedAt(record.get(Tables.ACTOR.UPDATED_AT).toEpochSecond())
      .withResourceRequirements(
        if (record.get(Tables.ACTOR.RESOURCE_REQUIREMENTS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.ACTOR.RESOURCE_REQUIREMENTS).data(),
            ScopedResourceRequirements::class.java,
          )
        },
      )

  /**
   * Build destination from db record.
   *
   * @param record db record
   * @return destination
   */
  fun buildDestinationConnection(record: Record): DestinationConnection =
    DestinationConnection()
      .withDestinationId(record.get(Tables.ACTOR.ID))
      .withConfiguration(Jsons.deserialize(record.get(Tables.ACTOR.CONFIGURATION).data()))
      .withWorkspaceId(record.get(Tables.ACTOR.WORKSPACE_ID))
      .withDestinationDefinitionId(record.get(Tables.ACTOR.ACTOR_DEFINITION_ID))
      .withTombstone(record.get(Tables.ACTOR.TOMBSTONE))
      .withName(record.get(Tables.ACTOR.NAME))
      .withCreatedAt(record.get(Tables.ACTOR.CREATED_AT).toEpochSecond())
      .withUpdatedAt(record.get(Tables.ACTOR.UPDATED_AT).toEpochSecond())
      .withResourceRequirements(
        if (record.get(Tables.ACTOR.RESOURCE_REQUIREMENTS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.ACTOR.RESOURCE_REQUIREMENTS).data(),
            ScopedResourceRequirements::class.java,
          )
        },
      )

  /**
   * Build source definition from db record.
   *
   * @param record db record
   * @return source definition
   */
  fun buildStandardSourceDefinition(
    record: Record,
    defaultMaxSecondsBetweenMessages: Long,
  ): StandardSourceDefinition {
    var maxSecondsBetweenMessage =
      if (record.get(Tables.ACTOR_DEFINITION.MAX_SECONDS_BETWEEN_MESSAGES) == null) {
        defaultMaxSecondsBetweenMessages
      } else {
        record.get(Tables.ACTOR_DEFINITION.MAX_SECONDS_BETWEEN_MESSAGES).toLong()
      }

    // All sources are starting to set this field according to their rate limits. As a
    // safeguard for sources with rate limits that are too low e.g. minutes etc, we default to
    // our defaults. One day, we'll relax this, be conservative for now.
    if (maxSecondsBetweenMessage < defaultMaxSecondsBetweenMessages) {
      maxSecondsBetweenMessage = defaultMaxSecondsBetweenMessages
    }

    return StandardSourceDefinition()
      .withSourceDefinitionId(record.get(Tables.ACTOR_DEFINITION.ID))
      .withDefaultVersionId(record.get(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID))
      .withIcon(record.get(Tables.ACTOR_DEFINITION.ICON))
      .withIconUrl(record.get(Tables.ACTOR_DEFINITION.ICON_URL))
      .withName(record.get(Tables.ACTOR_DEFINITION.NAME))
      .withSourceType(
        if (record.get(Tables.ACTOR_DEFINITION.SOURCE_TYPE) == null) {
          null
        } else {
          record
            .get(
              Tables.ACTOR_DEFINITION.SOURCE_TYPE,
              String::class.java,
            ).toEnum<StandardSourceDefinition.SourceType>()!!
        },
      ).withTombstone(record.get(Tables.ACTOR_DEFINITION.TOMBSTONE))
      .withPublic(record.get(Tables.ACTOR_DEFINITION.PUBLIC))
      .withCustom(record.get(Tables.ACTOR_DEFINITION.CUSTOM))
      .withEnterprise(record.get(Tables.ACTOR_DEFINITION.ENTERPRISE))
      .withResourceRequirements(
        if (record.get(Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS).data(),
            ScopedResourceRequirements::class.java,
          )
        },
      ).withMetrics(
        if (record.get(Tables.ACTOR_DEFINITION.METRICS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.ACTOR_DEFINITION.METRICS).data(),
            ConnectorRegistryEntryMetrics::class.java,
          )
        },
      ).withMaxSecondsBetweenMessages(maxSecondsBetweenMessage)
  }

  /**
   * Build destination definition from db record.
   *
   * @param record db record
   * @return destination definition
   */
  fun buildStandardDestinationDefinition(record: Record): StandardDestinationDefinition =
    StandardDestinationDefinition()
      .withDestinationDefinitionId(record.get(Tables.ACTOR_DEFINITION.ID))
      .withDefaultVersionId(record.get(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID))
      .withIcon(record.get(Tables.ACTOR_DEFINITION.ICON))
      .withIconUrl(record.get(Tables.ACTOR_DEFINITION.ICON_URL))
      .withName(record.get(Tables.ACTOR_DEFINITION.NAME))
      .withTombstone(record.get(Tables.ACTOR_DEFINITION.TOMBSTONE))
      .withPublic(record.get(Tables.ACTOR_DEFINITION.PUBLIC))
      .withCustom(record.get(Tables.ACTOR_DEFINITION.CUSTOM))
      .withEnterprise(record.get(Tables.ACTOR_DEFINITION.ENTERPRISE))
      .withMetrics(
        if (record.get(Tables.ACTOR_DEFINITION.METRICS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.ACTOR_DEFINITION.METRICS).data(),
            ConnectorRegistryEntryMetrics::class.java,
          )
        },
      ).withResourceRequirements(
        if (record.get(Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS).data(),
            ScopedResourceRequirements::class.java,
          )
        },
      )

  /**
   * Build destination oauth parameters from db record.
   *
   * @param record db record
   * @return destination oauth parameter
   */
  fun buildDestinationOAuthParameter(record: Record): DestinationOAuthParameter =
    DestinationOAuthParameter()
      .withOauthParameterId(record.get(Tables.ACTOR_OAUTH_PARAMETER.ID))
      .withConfiguration(Jsons.deserialize(record.get(Tables.ACTOR_OAUTH_PARAMETER.CONFIGURATION).data()))
      .withWorkspaceId(record.get(Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID))
      .withOrganizationId(record.get(Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID))
      .withDestinationDefinitionId(record.get(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID))

  /**
   * Build source oauth parameters from db record.
   *
   * @param record db record
   * @return source oauth parameters
   */
  fun buildSourceOAuthParameter(record: Record): SourceOAuthParameter =
    SourceOAuthParameter()
      .withOauthParameterId(record.get(Tables.ACTOR_OAUTH_PARAMETER.ID))
      .withConfiguration(Jsons.deserialize(record.get(Tables.ACTOR_OAUTH_PARAMETER.CONFIGURATION).data()))
      .withWorkspaceId(record.get(Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID))
      .withOrganizationId(record.get(Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID))
      .withSourceDefinitionId(record.get(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID))

  /**
   * Build actor catalog from db record.
   *
   * @param record db record
   * @return actor catalog
   */
  fun buildActorCatalog(record: Record): ActorCatalog =
    ActorCatalog()
      .withId(record.get(Tables.ACTOR_CATALOG.ID))
      .withCatalog(
        if (record.get(Tables.ACTOR_CATALOG.CATALOG) == null) {
          null
        } else {
          Jsons.deserialize(record.get(Tables.ACTOR_CATALOG.CATALOG).toString())
        },
      ).withCatalogType(
        if (record.get(Tables.ACTOR_CATALOG.CATALOG_TYPE) != null) {
          record
            .get(
              Tables.ACTOR_CATALOG.CATALOG_TYPE,
              String::class.java,
            ).toEnum<CatalogType>()!!
        } else {
          null
        },
      ).withCatalogHash(record.get(Tables.ACTOR_CATALOG.CATALOG_HASH))

  /**
   * Build actor catalog with updated at from db record.
   *
   * @param record db record
   * @return actor catalog with last updated at
   */
  fun buildActorCatalogWithUpdatedAt(record: Record): ActorCatalogWithUpdatedAt =
    ActorCatalogWithUpdatedAt()
      .withId(record.get(Tables.ACTOR_CATALOG.ID))
      .withCatalog(Jsons.jsonNode(parseAirbyteCatalog(record.get(Tables.ACTOR_CATALOG.CATALOG).toString())))
      .withCatalogHash(record.get(Tables.ACTOR_CATALOG.CATALOG_HASH))
      .withUpdatedAt(
        record
          .get(
            Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT,
            LocalDateTime::class.java,
          ).toEpochSecond(ZoneOffset.UTC),
      )

  /**
   * Parse airbyte catalog from JSON string.
   *
   * @param airbyteCatalogString catalog as JSON string
   * @return airbyte catalog
   */
  fun parseAirbyteCatalog(airbyteCatalogString: String): AirbyteCatalog = Jsons.deserialize(airbyteCatalogString, AirbyteCatalog::class.java)

  /**
   * Build actor catalog fetch event from db record.
   *
   * @param record db record
   * @return actor catalog fetch event
   */
  fun buildActorCatalogFetchEvent(record: Record): ActorCatalogFetchEvent =
    ActorCatalogFetchEvent()
      .withActorId(record.get(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID))
      .withActorCatalogId(record.get(Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID))
      .withCreatedAt(
        record
          .get(
            Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT,
            LocalDateTime::class.java,
          ).toEpochSecond(ZoneOffset.UTC),
      )

  /**
   * Build workspace service account from db record.
   *
   * @param record db record
   * @return workspace service account
   */
  fun buildWorkspaceServiceAccount(record: Record): WorkspaceServiceAccount =
    WorkspaceServiceAccount()
      .withWorkspaceId(record.get(Tables.WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID))
      .withServiceAccountId(record.get(Tables.WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_ID))
      .withServiceAccountEmail(record.get(Tables.WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_EMAIL))
      .withJsonCredential(
        if (record.get(Tables.WORKSPACE_SERVICE_ACCOUNT.JSON_CREDENTIAL) == null) {
          null
        } else {
          Jsons.deserialize(record.get(Tables.WORKSPACE_SERVICE_ACCOUNT.JSON_CREDENTIAL).data())
        },
      ).withHmacKey(
        if (record.get(Tables.WORKSPACE_SERVICE_ACCOUNT.HMAC_KEY) == null) {
          null
        } else {
          Jsons.deserialize(record.get(Tables.WORKSPACE_SERVICE_ACCOUNT.HMAC_KEY).data())
        },
      )

  /**
   * Builder connector builder with manifest project from db record.
   *
   * @param record db record
   * @return connector builder project
   */
  fun buildConnectorBuilderProject(record: Record): ConnectorBuilderProject =
    buildConnectorBuilderProjectWithoutManifestDraft(record)
      .withManifestDraft(
        if (record.get(Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT) == null) {
          null
        } else {
          Jsons.deserialize(record.get(Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT).data())
        },
      )

  /**
   * Builder connector builder without manifest project from db record.
   *
   * @param record db record
   * @return connector builder project
   */
  fun buildConnectorBuilderProjectWithoutManifestDraft(record: Record): ConnectorBuilderProject =
    ConnectorBuilderProject()
      .withWorkspaceId(record.get(Tables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID))
      .withUpdatedAt(record.get(Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT).toEpochSecond())
      .withBuilderProjectId(record.get(Tables.CONNECTOR_BUILDER_PROJECT.ID))
      .withName(record.get(Tables.CONNECTOR_BUILDER_PROJECT.NAME))
      .withHasDraft(record["hasDraft"] as Boolean?)
      .withTombstone(record.get(Tables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE))
      .withActorDefinitionId(record.get(Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID))
      .withActiveDeclarativeManifestVersion(record.get(Tables.ACTIVE_DECLARATIVE_MANIFEST.VERSION))
      .withTestingValues(
        if (record.get(Tables.CONNECTOR_BUILDER_PROJECT.TESTING_VALUES) == null) {
          null
        } else {
          Jsons.deserialize(record.get(Tables.CONNECTOR_BUILDER_PROJECT.TESTING_VALUES).data())
        },
      ).withBaseActorDefinitionVersionId(record.get(Tables.CONNECTOR_BUILDER_PROJECT.BASE_ACTOR_DEFINITION_VERSION_ID))
      .withContributionPullRequestUrl(record.get(Tables.CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_PULL_REQUEST_URL))
      .withComponentsFileContent(record.get(Tables.CONNECTOR_BUILDER_PROJECT.COMPONENTS_FILE_CONTENT))
      .withContributionActorDefinitionId(record.get(Tables.CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_ACTOR_DEFINITION_ID))

  /**
   * Builder declarative manifest from db record.
   *
   * @param record db record
   * @return declarative manifest
   */
  fun buildDeclarativeManifest(record: Record): DeclarativeManifest =
    buildDeclarativeManifestWithoutManifestAndSpec(record)
      .withManifest(
        Jsons.deserialize(
          record.get(Tables.DECLARATIVE_MANIFEST.MANIFEST).data(),
        ),
      ).withSpec(Jsons.deserialize(record.get(Tables.DECLARATIVE_MANIFEST.SPEC).data()))

  /**
   * Builder declarative manifest without manifest from db record.
   *
   * @param record db record
   * @return declarative manifest
   */
  fun buildDeclarativeManifestWithoutManifestAndSpec(record: Record): DeclarativeManifest =
    DeclarativeManifest()
      .withActorDefinitionId(record.get(Tables.DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
      .withDescription(record.get(Tables.DECLARATIVE_MANIFEST.DESCRIPTION))
      .withVersion(record.get(Tables.DECLARATIVE_MANIFEST.VERSION))
      .withComponentsFileContent(record.get(Tables.DECLARATIVE_MANIFEST.COMPONENTS_FILE_CONTENT))

  /**
   * Actor definition config injection from db record.
   *
   * @param record db record
   * @return actor definition config injection
   */
  fun buildActorDefinitionConfigInjection(record: Record): ActorDefinitionConfigInjection =
    ActorDefinitionConfigInjection()
      .withActorDefinitionId(record.get(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID))
      .withInjectionPath(record.get(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.INJECTION_PATH))
      .withJsonToInject(Jsons.deserialize(record.get(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.JSON_TO_INJECT).data()))

  /**
   * Actor definition breaking change from db record.
   *
   * @param record db record
   * @return actor definition breaking change
   */
  fun buildActorDefinitionBreakingChange(record: Record): ActorDefinitionBreakingChange {
    val scopedImpact: MutableList<BreakingChangeScope> = ArrayList()
    if (record.get(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.SCOPED_IMPACT) != null) {
      scopedImpact.addAll(
        Jsons.deserialize(
          record.get(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.SCOPED_IMPACT).data(),
          object : TypeReference<List<BreakingChangeScope>>() {},
        ),
      )
    }
    return ActorDefinitionBreakingChange()
      .withActorDefinitionId(record.get(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID))
      .withVersion(Version(record.get(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.VERSION)))
      .withMessage(record.get(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MESSAGE))
      .withUpgradeDeadline(record.get(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPGRADE_DEADLINE).toString())
      .withMigrationDocumentationUrl(record.get(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MIGRATION_DOCUMENTATION_URL))
      .withDeadlineAction(record.get(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.DEADLINE_ACTION))
      .withScopedImpact(scopedImpact)
  }

  /**
   * Actor definition version from a db record.
   *
   * @param record db record
   * @return actor definition version
   */
  @JvmStatic
  fun buildActorDefinitionVersion(record: Record): ActorDefinitionVersion =
    ActorDefinitionVersion()
      .withVersionId(record.get(Tables.ACTOR_DEFINITION_VERSION.ID))
      .withActorDefinitionId(record.get(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID))
      .withDockerRepository(record.get(Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY))
      .withDockerImageTag(record.get(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG))
      .withSpec(
        Jsons.deserialize(
          record.get(Tables.ACTOR_DEFINITION_VERSION.SPEC).data(),
          ConnectorSpecification::class.java,
        ),
      ).withDocumentationUrl(record.get(Tables.ACTOR_DEFINITION_VERSION.DOCUMENTATION_URL))
      .withSupportLevel(
        if (record.get(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL) == null) {
          null
        } else {
          record
            .get(
              Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
              String::class.java,
            ).toEnum<SupportLevel>()!!
        },
      ).withProtocolVersion(AirbyteProtocolVersion.getWithDefault(record.get(Tables.ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION)).serialize())
      .withReleaseStage(
        if (record.get(Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE) == null) {
          null
        } else {
          record
            .get(
              Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE,
              String::class.java,
            ).toEnum<ReleaseStage>()!!
        },
      ).withReleaseDate(
        if (record.get(Tables.ACTOR_DEFINITION_VERSION.RELEASE_DATE) == null) {
          null
        } else {
          record.get(Tables.ACTOR_DEFINITION_VERSION.RELEASE_DATE).toString()
        },
      ).withLastPublished(
        if (record.get(Tables.ACTOR_DEFINITION_VERSION.LAST_PUBLISHED) == null) {
          null
        } else {
          Date.from(record.get(Tables.ACTOR_DEFINITION_VERSION.LAST_PUBLISHED).toInstant())
        },
      ).withCdkVersion(record.get(Tables.ACTOR_DEFINITION_VERSION.CDK_VERSION))
      .withAllowedHosts(
        if (record.get(Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS).data(),
            AllowedHosts::class.java,
          )
        },
      ).withSuggestedStreams(
        if (record.get(Tables.ACTOR_DEFINITION_VERSION.SUGGESTED_STREAMS) == null) {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.ACTOR_DEFINITION_VERSION.SUGGESTED_STREAMS).data(),
            SuggestedStreams::class.java,
          )
        },
      ).withSupportsRefreshes(record.get(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_REFRESHES))
      .withSupportState(
        record
          .get(
            Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
            String::class.java,
          ).toEnum<ActorDefinitionVersion.SupportState>()!!,
      ).withInternalSupportLevel(
        record.get(
          Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL,
          Long::class.java,
        ),
      ).withLanguage(record.get(Tables.ACTOR_DEFINITION_VERSION.LANGUAGE))
      .withSupportsFileTransfer(record.get(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_FILE_TRANSFER))
      .withSupportsDataActivation(record.get(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_DATA_ACTIVATION))
      .withConnectorIPCOptions(
        if (record.get(Tables.ACTOR_DEFINITION_VERSION.CONNECTOR_IPC_OPTIONS) == null) {
          null
        } else {
          Jsons.deserialize(record.get(Tables.ACTOR_DEFINITION_VERSION.CONNECTOR_IPC_OPTIONS).data())
        },
      )

  fun buildSecretPersistenceCoordinate(record: Record): SecretPersistenceCoordinate =
    SecretPersistenceCoordinate()
      .withCoordinate(record.get(Tables.SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_CONFIG_COORDINATE))
      .withScopeId(record.get(Tables.SECRET_PERSISTENCE_CONFIG.SCOPE_ID))
      .withScopeType(
        record
          .get(
            Tables.SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE,
            String::class.java,
          ).toEnum<ScopeType>()!!,
      ).withSecretPersistenceType(
        record
          .get(
            Tables.SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_TYPE,
            String::class.java,
          ).toEnum<SecretPersistenceConfig.SecretPersistenceType>()!!,
      )
}
