/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.UUID

class ScopedConfigurationMapperTest {
  @Test
  fun `toEntity passes resourceType and resourceId through when set`() {
    val resourceId = UUID.randomUUID()
    val model =
      ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withKey("connector_version")
        .withValue("1.0.0")
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(UUID.randomUUID())
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withResourceId(resourceId)
        .withOriginType(ConfigOriginType.USER)
        .withOrigin(UUID.randomUUID().toString())

    val entity = model.toEntity()

    assertEquals(io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType.actor_definition, entity.resourceType)
    assertEquals(resourceId, entity.resourceId)
  }

  @Test
  fun `toEntity allows null resourceType and resourceId (PrivateLink scoped configs)`() {
    val model =
      ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withKey("network_security_token")
        .withValue("token-value")
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(UUID.randomUUID())
        .withOriginType(ConfigOriginType.PRIVATE_LINK)
        .withOrigin(UUID.randomUUID().toString())

    val entity = model.toEntity()

    assertNull(entity.resourceType)
    assertNull(entity.resourceId)
  }
}
