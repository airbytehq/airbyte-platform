package io.airbyte.api.server.helpers

import io.airbyte.airbyte_api.model.generated.NamespaceDefinitionEnum
import io.airbyte.airbyte_api.model.generated.NonBreakingSchemaUpdatesBehaviorEnum
import io.airbyte.api.client.model.generated.NamespaceDefinitionType
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ConnectionHelperTest {
  @Test
  fun testConvertNonBreakingSchemaUpdatesBehaviorEnum() {
    val result = ConnectionHelper.convertNonBreakingSchemaUpdatesBehaviorEnum(NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION)
    assertEquals(NonBreakingChangesPreference.DISABLE, result)

    val result2 = ConnectionHelper.convertNonBreakingSchemaUpdatesBehaviorEnum(NonBreakingSchemaUpdatesBehaviorEnum.PROPAGATE_FULLY)
    assertEquals(NonBreakingChangesPreference.PROPAGATE_FULLY, result2)
  }

  @Test
  fun testConvertNamespaceDefinitionEnum() {
    val result = ConnectionHelper.convertNamespaceDefinitionEnum(NamespaceDefinitionEnum.CUSTOM_FORMAT)
    assertEquals(NamespaceDefinitionType.CUSTOMFORMAT, result)

    val result2 = ConnectionHelper.convertNamespaceDefinitionEnum(NamespaceDefinitionEnum.SOURCE)
    assertEquals(NamespaceDefinitionType.SOURCE, result2)
  }
}
