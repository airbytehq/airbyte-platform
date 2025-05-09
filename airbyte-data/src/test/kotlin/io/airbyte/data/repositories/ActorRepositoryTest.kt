/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.Actor
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class ActorRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making workspaces as well
      jooqDslContext
        .alterTable(
          Tables.ACTOR,
        ).dropForeignKey(Keys.ACTOR__ACTOR_WORKSPACE_ID_FKEY.constraint())
        .execute()
      jooqDslContext
        .alterTable(
          Tables.ACTOR,
        ).dropForeignKey(Keys.ACTOR__ACTOR_ACTOR_DEFINITION_ID_FKEY.constraint())
        .execute()
    }
  }

  @Test
  fun testFindByActorId() {
    val name = "test-name"
    val actor =
      Actor(
        name = name,
        workspaceId = UUID.randomUUID(),
        actorDefinitionId = UUID.randomUUID(),
        configuration = Jsons.emptyObject(),
        actorType = ActorType.source,
        tombstone = false,
      )

    val savedActor = actorRepository.save(actor)

    val foundActor = actorRepository.findByActorId(savedActor.id!!)

    assertEquals(name, foundActor?.name)
  }
}
