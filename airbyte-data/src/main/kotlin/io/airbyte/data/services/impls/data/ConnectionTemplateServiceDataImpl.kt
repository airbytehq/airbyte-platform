/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectionTemplate
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSync.NonBreakingChangesPreference
import io.airbyte.data.repositories.ConnectionTemplateRepository
import io.airbyte.data.services.ConnectionTemplateService
import io.airbyte.data.services.impls.data.mappers.EntityConnectionTemplate
import io.airbyte.data.services.impls.data.mappers.camelToSnake
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.db.instance.configs.jooq.generated.enums.NonBreakingChangePreferenceType
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import jakarta.inject.Singleton

@Singleton
open class ConnectionTemplateServiceDataImpl(
  private val repository: ConnectionTemplateRepository,
) : ConnectionTemplateService {
  override fun createTemplate(
    organizationId: OrganizationId,
    destinationName: String,
    actorDefinitionId: ActorDefinitionId,
    destinationConfig: JsonNode,
    namespaceDefinitionType: NamespaceDefinitionType,
    namespaceFormat: String?,
    prefix: String?,
    scheduleData: ScheduleData?,
    resourceRequirements: ResourceRequirements?,
    nonBreakingChangesPreference: NonBreakingChangesPreference,
    syncOnCreate: Boolean,
  ): ConnectionTemplate {
    // FIXME: there should be a check preventing from creating a template with the same name https://github.com/airbytehq/airbyte-internal-issues/issues/12818
    // Duplicates cannot be allowed because we lookup by name when creating the connection.
    val scheduleType = inferScheduleType(scheduleData).value()
    val entity =
      EntityConnectionTemplate(
        organizationId = organizationId.value,
        destinationName = destinationName,
        // FIXME we eventually want to support passing destinationType instead of actor definition ID https://github.com/airbytehq/airbyte-internal-issues/issues/12811
        destinationDefinitionId = actorDefinitionId.value,
        destinationConfig = destinationConfig,
        namespaceDefinition =
          io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
            .valueOf(namespaceDefinitionType.value()),
        namespaceFormat = namespaceFormat,
        prefix = prefix,
        scheduleType =
          io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType
            .valueOf(camelToSnake(scheduleType)),
        scheduleData = Jsons.jsonNode(scheduleData),
        resourceRequirements = Jsons.jsonNode(resourceRequirements),
        nonBreakingChangesPreference = NonBreakingChangePreferenceType.valueOf(nonBreakingChangesPreference.value()),
        syncOnCreate = syncOnCreate,
      )

    // FIXME: we shouldn't store secrets in the DB https://github.com/airbytehq/airbyte-internal-issues/issues/12819
    val connectionTemplate = repository.save(entity).toConfigModel()
    return connectionTemplate
  }

  private fun inferScheduleType(scheduleData: ScheduleData?): StandardSync.ScheduleType =
    if (scheduleData == null) {
      StandardSync.ScheduleType.MANUAL
    } else if (scheduleData.basicSchedule != null) {
      throw IllegalArgumentException("Basic schedule is not supported in connection templates.")
    } else if (scheduleData.cron == null) {
      StandardSync.ScheduleType.MANUAL
    } else {
      StandardSync.ScheduleType.CRON
    }
}
