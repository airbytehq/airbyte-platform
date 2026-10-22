/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.server.generated.models.NamespaceDefinitionType
import io.airbyte.api.server.generated.models.NonBreakingChangesPreference
import io.airbyte.api.server.generated.models.Tag
import io.airbyte.publicApi.server.generated.models.NamespaceDefinitionEnum
import io.airbyte.publicApi.server.generated.models.NamespaceDefinitionEnumNoDefault
import io.airbyte.publicApi.server.generated.models.NonBreakingSchemaUpdatesBehaviorEnum
import io.airbyte.publicApi.server.generated.models.NonBreakingSchemaUpdatesBehaviorEnumNoDefault

/**
 * Connection helpers.
 */
object ConnectionHelper {
  /**
   * Convert namespace definition enum -> NamespaceDefinitionType.
   */
  fun convertNamespaceDefinitionEnum(namespaceDefinitionEnum: NamespaceDefinitionEnum?): NamespaceDefinitionType =
    if (namespaceDefinitionEnum === NamespaceDefinitionEnum.CUSTOM_FORMAT) {
      NamespaceDefinitionType.CUSTOMFORMAT
    } else {
      NamespaceDefinitionType.decode(namespaceDefinitionEnum.toString())
        ?: throw IllegalArgumentException("Unexpected namespace definition: $namespaceDefinitionEnum")
    }

  /**
   * Convert namespace definition enum -> NamespaceDefinitionType.
   */
  fun convertNamespaceDefinitionEnum(namespaceDefinitionEnum: NamespaceDefinitionEnumNoDefault?): NamespaceDefinitionType =
    if (namespaceDefinitionEnum === NamespaceDefinitionEnumNoDefault.CUSTOM_FORMAT) {
      NamespaceDefinitionType.CUSTOMFORMAT
    } else {
      NamespaceDefinitionType.decode(namespaceDefinitionEnum.toString())
        ?: throw IllegalArgumentException("Unexpected namespace definition: $namespaceDefinitionEnum")
    }

  /**
   * Convert non-breaking schema updates behavior enum -> NonBreakingChangesPreference.
   */
  fun convertNonBreakingSchemaUpdatesBehaviorEnum(
    nonBreakingSchemaUpdatesBehaviorEnum: NonBreakingSchemaUpdatesBehaviorEnum?,
  ): NonBreakingChangesPreference =
    if (nonBreakingSchemaUpdatesBehaviorEnum === NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION) {
      NonBreakingChangesPreference.DISABLE
    } else {
      NonBreakingChangesPreference.decode(nonBreakingSchemaUpdatesBehaviorEnum.toString())
        ?: throw IllegalArgumentException("Unexpected non-breaking schema updates behavior: $nonBreakingSchemaUpdatesBehaviorEnum")
    }

  /**
   * Convert non-breaking schema updates behavior enum -> NonBreakingChangesPreference.
   */
  fun convertNonBreakingSchemaUpdatesBehaviorEnum(
    nonBreakingSchemaUpdatesBehaviorEnum: NonBreakingSchemaUpdatesBehaviorEnumNoDefault?,
  ): NonBreakingChangesPreference =
    if (nonBreakingSchemaUpdatesBehaviorEnum === NonBreakingSchemaUpdatesBehaviorEnumNoDefault.DISABLE_CONNECTION) {
      NonBreakingChangesPreference.DISABLE
    } else {
      NonBreakingChangesPreference.decode(nonBreakingSchemaUpdatesBehaviorEnum.toString())
        ?: throw IllegalArgumentException("Unexpected non-breaking schema updates behavior: $nonBreakingSchemaUpdatesBehaviorEnum")
    }

  fun convertTags(tags: List<io.airbyte.publicApi.server.generated.models.Tag>): List<Tag> =
    tags.map {
      Tag(tagId = it.tagId, name = it.name, color = it.color, workspaceId = it.workspaceId)
    }
}
