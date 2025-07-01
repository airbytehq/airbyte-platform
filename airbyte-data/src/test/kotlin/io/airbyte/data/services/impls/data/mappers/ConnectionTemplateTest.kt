/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.commons.json.Jsons
import io.airbyte.config.BasicSchedule
import io.airbyte.config.ConnectionTemplate
import io.airbyte.config.Cron
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync.NonBreakingChangesPreference
import io.airbyte.config.StandardSync.ScheduleType
import io.airbyte.db.instance.configs.jooq.generated.enums.NonBreakingChangePreferenceType
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionTemplateTest {
  private val organizationId = OrganizationId(UUID.randomUUID())
  private val destinationName = "destination_name"
  private val destinationDefinitionId = ActorDefinitionId(UUID.randomUUID())
  private val destinationConfig = objectMapper.readTree("{}")
  private val namespaceDefinitionType = NamespaceDefinitionType.DESTINATION
  private val ignoreNonBreakingChangesPreference = NonBreakingChangesPreference.IGNORE
  private val disableNonBreakingChangesPreference = NonBreakingChangesPreference.DISABLE
  private val propagateColumnsNonBreakingChangesPreference = NonBreakingChangesPreference.PROPAGATE_COLUMNS
  private val propagateFullyNonBreakingChangesPreference = NonBreakingChangesPreference.PROPAGATE_FULLY
  private val connectionTemplateId = UUID.randomUUID()
  private val basicScheduleData = ScheduleData().withBasicSchedule(BasicSchedule().withTimeUnit(BasicSchedule.TimeUnit.HOURS).withUnits(24L))
  private val cronScheduleData = ScheduleData().withCron(Cron().withCronExpression("0 0 * * *").withCronTimeZone("UTC"))
  private val resourceRequirements =
    ResourceRequirements()
      .withCpuLimit(
        "1.0",
      ).withCpuRequest("1.0")
      .withMemoryLimit("1g")
      .withMemoryRequest("0.5g")
      .withEphemeralStorageLimit("1g")
      .withEphemeralStorageRequest("1g")

  @Test
  fun `test convert`() {
    val entity =
      EntityConnectionTemplate(
        connectionTemplateId,
        organizationId.value,
        destinationName,
        destinationDefinitionId.value,
        destinationConfig,
        io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
          .valueOf(namespaceDefinitionType.value()),
        null,
        null,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.cron,
        Jsons.jsonNode(cronScheduleData),
        null,
        NonBreakingChangePreferenceType.valueOf(ignoreNonBreakingChangesPreference.value()),
        syncOnCreate = true,
      )

    val configModel = entity.toConfigModel()

    val expectedConfigTemplate =
      ConnectionTemplate(
        id = connectionTemplateId,
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = destinationDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.CRON,
        scheduleData = cronScheduleData,
        resourceRequirements = null,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        syncOnCreate = true,
      )

    assertEquals(expectedConfigTemplate, configModel)
  }

  @Test
  fun `test convert basic schedule`() {
    val entity =
      EntityConnectionTemplate(
        connectionTemplateId,
        organizationId.value,
        destinationName,
        destinationDefinitionId.value,
        destinationConfig,
        io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
          .valueOf(namespaceDefinitionType.value()),
        null,
        null,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.basic_schedule,
        Jsons.jsonNode(basicScheduleData),
        null,
        NonBreakingChangePreferenceType.valueOf(ignoreNonBreakingChangesPreference.value()),
        syncOnCreate = true,
      )

    val configModel = entity.toConfigModel()

    val expectedConfigTemplate =
      ConnectionTemplate(
        id = connectionTemplateId,
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = destinationDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.BASIC_SCHEDULE,
        scheduleData = basicScheduleData,
        resourceRequirements = null,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        syncOnCreate = true,
      )

    assertEquals(expectedConfigTemplate, configModel)
  }

  @Test
  fun `test convert manual schedule`() {
    val entity =
      EntityConnectionTemplate(
        connectionTemplateId,
        organizationId.value,
        destinationName,
        destinationDefinitionId.value,
        destinationConfig,
        io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
          .valueOf(namespaceDefinitionType.value()),
        null,
        null,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.manual,
        null,
        null,
        NonBreakingChangePreferenceType.valueOf(ignoreNonBreakingChangesPreference.value()),
        syncOnCreate = false,
      )

    val configModel = entity.toConfigModel()

    val expectedConfigTemplate =
      ConnectionTemplate(
        id = connectionTemplateId,
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = destinationDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.MANUAL,
        scheduleData = null,
        resourceRequirements = null,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        syncOnCreate = false,
      )

    assertEquals(expectedConfigTemplate, configModel)
  }

  @Test
  fun `test convert non breaking changes preference`() {
    val entity =
      EntityConnectionTemplate(
        connectionTemplateId,
        organizationId.value,
        destinationName,
        destinationDefinitionId.value,
        destinationConfig,
        io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
          .valueOf(namespaceDefinitionType.value()),
        null,
        null,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.cron,
        Jsons.jsonNode(cronScheduleData),
        null,
        NonBreakingChangePreferenceType.disable,
        syncOnCreate = false,
      )

    val configModel = entity.toConfigModel()

    val expectedConfigTemplate =
      ConnectionTemplate(
        id = connectionTemplateId,
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = destinationDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.CRON,
        scheduleData = cronScheduleData,
        resourceRequirements = null,
        nonBreakingChangesPreference = disableNonBreakingChangesPreference,
        syncOnCreate = false,
      )

    assertEquals(expectedConfigTemplate, configModel)
  }

  @Test
  fun `test convert non breaking changes propagate columns`() {
    val entity =
      EntityConnectionTemplate(
        connectionTemplateId,
        organizationId.value,
        destinationName,
        destinationDefinitionId.value,
        destinationConfig,
        io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
          .valueOf(namespaceDefinitionType.value()),
        null,
        null,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.cron,
        Jsons.jsonNode(cronScheduleData),
        null,
        NonBreakingChangePreferenceType.propagate_columns,
        syncOnCreate = false,
      )

    val configModel = entity.toConfigModel()

    val expectedConfigTemplate =
      ConnectionTemplate(
        id = connectionTemplateId,
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = destinationDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.CRON,
        scheduleData = cronScheduleData,
        resourceRequirements = null,
        nonBreakingChangesPreference = propagateColumnsNonBreakingChangesPreference,
        syncOnCreate = false,
      )

    assertEquals(expectedConfigTemplate, configModel)
  }

  @Test
  fun `test convert non breaking changes propagate fully`() {
    val entity =
      EntityConnectionTemplate(
        connectionTemplateId,
        organizationId.value,
        destinationName,
        destinationDefinitionId.value,
        destinationConfig,
        io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
          .valueOf(namespaceDefinitionType.value()),
        null,
        null,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.cron,
        Jsons.jsonNode(cronScheduleData),
        null,
        NonBreakingChangePreferenceType.propagate_fully,
        syncOnCreate = false,
      )

    val configModel = entity.toConfigModel()

    val expectedConfigTemplate =
      ConnectionTemplate(
        id = connectionTemplateId,
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = destinationDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.CRON,
        scheduleData = cronScheduleData,
        resourceRequirements = null,
        nonBreakingChangesPreference = propagateFullyNonBreakingChangesPreference,
        syncOnCreate = false,
      )

    assertEquals(expectedConfigTemplate, configModel)
  }

  @Test
  fun `test convert with resource requirements`() {
    val entity =
      EntityConnectionTemplate(
        connectionTemplateId,
        organizationId.value,
        destinationName,
        destinationDefinitionId.value,
        destinationConfig,
        io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
          .valueOf(namespaceDefinitionType.value()),
        null,
        null,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.cron,
        Jsons.jsonNode(cronScheduleData),
        Jsons.jsonNode(resourceRequirements),
        NonBreakingChangePreferenceType.valueOf(ignoreNonBreakingChangesPreference.value()),
        syncOnCreate = false,
      )

    val configModel = entity.toConfigModel()

    val expectedConfigTemplate =
      ConnectionTemplate(
        id = connectionTemplateId,
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = destinationDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.CRON,
        scheduleData = cronScheduleData,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        syncOnCreate = false,
      )

    assertEquals(expectedConfigTemplate, configModel)
  }
}
