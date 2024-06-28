/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
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
  fun convertNamespaceDefinitionEnum(namespaceDefinitionEnum: NamespaceDefinitionEnum?): NamespaceDefinitionType {
    return if (namespaceDefinitionEnum === NamespaceDefinitionEnum.CUSTOM_FORMAT) {
      NamespaceDefinitionType.CUSTOMFORMAT
    } else {
      NamespaceDefinitionType.fromValue(namespaceDefinitionEnum.toString())
    }
  }

  /**
   * Convert namespace definition enum -> NamespaceDefinitionType.
   */
  fun convertNamespaceDefinitionEnum(namespaceDefinitionEnum: NamespaceDefinitionEnumNoDefault?): NamespaceDefinitionType {
    return if (namespaceDefinitionEnum === NamespaceDefinitionEnumNoDefault.CUSTOM_FORMAT) {
      NamespaceDefinitionType.CUSTOMFORMAT
    } else {
      NamespaceDefinitionType.fromValue(namespaceDefinitionEnum.toString())
    }
  }

  /**
   * Convert non-breaking schema updates behavior enum -> NonBreakingChangesPreference.
   */
  fun convertNonBreakingSchemaUpdatesBehaviorEnum(
    nonBreakingSchemaUpdatesBehaviorEnum: NonBreakingSchemaUpdatesBehaviorEnum?,
  ): NonBreakingChangesPreference {
    return if (nonBreakingSchemaUpdatesBehaviorEnum === NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION) {
      NonBreakingChangesPreference.DISABLE
    } else {
      NonBreakingChangesPreference.fromValue(nonBreakingSchemaUpdatesBehaviorEnum.toString())
    }
  }

  /**
   * Convert non-breaking schema updates behavior enum -> NonBreakingChangesPreference.
   */
  fun convertNonBreakingSchemaUpdatesBehaviorEnum(
    nonBreakingSchemaUpdatesBehaviorEnum: NonBreakingSchemaUpdatesBehaviorEnumNoDefault?,
  ): NonBreakingChangesPreference {
    return if (nonBreakingSchemaUpdatesBehaviorEnum === NonBreakingSchemaUpdatesBehaviorEnumNoDefault.DISABLE_CONNECTION) {
      NonBreakingChangesPreference.DISABLE
    } else {
      NonBreakingChangesPreference.fromValue(nonBreakingSchemaUpdatesBehaviorEnum.toString())
    }
  }
}
