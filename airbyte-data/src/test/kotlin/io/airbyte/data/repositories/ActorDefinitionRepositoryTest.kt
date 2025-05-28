/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ActorDefinition
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest
class ActorDefinitionRepositoryTest : AbstractConfigRepositoryTest() {
  @Test
  fun testFindByActorDefinitionId() {
    val name = "test-name"
    val actorDefinition =
      ActorDefinition(
        name = name,
        actorType = ActorType.source,
      )

    val savedActorDefinition = actorDefinitionRepository.save(actorDefinition)

    val foundActorDefinition = actorDefinitionRepository.findByActorDefinitionId(savedActorDefinition.id!!)

    Assertions.assertEquals(name, foundActorDefinition?.name)
  }
}
