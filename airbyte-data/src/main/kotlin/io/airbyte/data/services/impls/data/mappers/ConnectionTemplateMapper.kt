/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.commons.json.Jsons
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSync.NonBreakingChangesPreference
import io.airbyte.domain.models.OrganizationId
import java.util.Locale

typealias EntityConnectionTemplate = io.airbyte.data.repositories.entities.ConnectionTemplate
typealias ModelConnectionTemplate = io.airbyte.config.ConnectionTemplate

fun EntityConnectionTemplate.toConfigModel(): ModelConnectionTemplate =
  ModelConnectionTemplate(
    id = this.id!!,
    organizationId = OrganizationId(this.organizationId),
    destinationName = this.destinationName,
    destinationActorDefinitionId = this.destinationDefinitionId,
    destinationConfiguration = this.destinationConfig,
    namespaceDefinitionType = JobSyncConfig.NamespaceDefinitionType.fromValue(this.namespaceDefinition.literal),
    namespaceFormat = this.namespaceFormat,
    prefix = this.prefix,
    scheduleType = StandardSync.ScheduleType.fromValue(capitalizeAndToCamelCase(this.scheduleType.literal)),
    scheduleData = if (this.scheduleData != null) Jsons.deserialize(this.scheduleData.toString(), ScheduleData::class.java) else null,
    resourceRequirements = Jsons.deserialize(this.resourceRequirements.toString(), ResourceRequirements::class.java),
    nonBreakingChangesPreference = NonBreakingChangesPreference.valueOf(this.nonBreakingChangesPreference.literal.uppercase()),
    syncOnCreate = this.syncOnCreate,
  )

fun capitalizeAndToCamelCase(s: String): String =
  s
    .split("_")
    .joinToString("") { it.replaceFirstChar { char -> char.uppercase(Locale.getDefault()) } }

fun camelToSnake(s: String): String = s.replace(Regex("([a-z])([A-Z]+)"), "$1_$2").lowercase()
