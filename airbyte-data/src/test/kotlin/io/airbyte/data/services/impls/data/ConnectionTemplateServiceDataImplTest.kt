/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectionTemplate
import io.airbyte.config.Cron
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync.NonBreakingChangesPreference
import io.airbyte.config.StandardSync.ScheduleType
import io.airbyte.data.repositories.ConnectionTemplateRepository
import io.airbyte.data.services.impls.data.mappers.EntityConnectionTemplate
import io.airbyte.db.instance.configs.jooq.generated.enums.NonBreakingChangePreferenceType
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionTemplateServiceDataImplTest {
  private val objectMapper: ObjectMapper = ObjectMapper()

  private lateinit var repository: ConnectionTemplateRepository

  private lateinit var service: ConnectionTemplateServiceDataImpl

  private val organizationId = OrganizationId(UUID.randomUUID())
  private val destinationName = "destination_name"
  private val destinationDefinitionId = ActorDefinitionId(UUID.randomUUID())
  private val destinationConfig = objectMapper.readTree("{}")
  private val namespaceDefinitionType = NamespaceDefinitionType.DESTINATION
  private val ignoreNonBreakingChangesPreference = NonBreakingChangesPreference.IGNORE
  private val connectionTemplateId = UUID.randomUUID()
  private val cronScheduleData = ScheduleData().withCron(Cron().withCronExpression("0 0 * * *").withCronTimeZone("UTC"))
  private val syncOnCreate = true

  @BeforeEach
  fun setup() {
    repository = mockk()
    service = ConnectionTemplateServiceDataImpl(repository)
  }

  @Test
  fun `test create with destination definition id and manual schedule`() {
    val connectionTemplateConfigEntity =
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
        syncOnCreate,
      )

    every {
      repository.save(any())
    } returns connectionTemplateConfigEntity

    val templateCreated =
      service.createTemplate(
        organizationId,
        destinationName,
        destinationDefinitionId,
        destinationConfig,
        namespaceDefinitionType,
        null,
        null,
        null,
        null,
        ignoreNonBreakingChangesPreference,
        syncOnCreate,
      )

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
        syncOnCreate = syncOnCreate,
      )

    assertEquals(
      expectedConfigTemplate,
      templateCreated,
    )
  }

  @Test
  fun `test create with basic schedule`() {
    val connectionTemplateConfigEntity =
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
        syncOnCreate,
      )

    every {
      repository.save(any())
    } returns connectionTemplateConfigEntity

    val templateCreated =
      service.createTemplate(
        organizationId,
        destinationName,
        destinationDefinitionId,
        destinationConfig,
        namespaceDefinitionType,
        null,
        null,
        null,
        null,
        ignoreNonBreakingChangesPreference,
        syncOnCreate,
      )

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
        syncOnCreate = syncOnCreate,
      )

    assertEquals(
      expectedConfigTemplate,
      templateCreated,
    )
  }

  @Test
  fun `test create with cron schedule`() {
    val connectionTemplateConfigEntity =
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
        syncOnCreate,
      )

    every {
      repository.save(any())
    } returns connectionTemplateConfigEntity

    val templateCreated =
      service.createTemplate(
        organizationId,
        destinationName,
        destinationDefinitionId,
        destinationConfig,
        namespaceDefinitionType,
        null,
        null,
        cronScheduleData,
        null,
        ignoreNonBreakingChangesPreference,
        false,
      )

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
        syncOnCreate = false,
      )

    assertEquals(
      expectedConfigTemplate.scheduleType,
      templateCreated.scheduleType,
    )
    assertEquals(
      expectedConfigTemplate.scheduleData!!.cron.cronExpression,
      templateCreated.scheduleData!!.cron.cronExpression,
    )
    assertEquals(
      expectedConfigTemplate.scheduleData!!.cron.cronTimeZone,
      templateCreated.scheduleData!!.cron.cronTimeZone,
    )
  }
}
