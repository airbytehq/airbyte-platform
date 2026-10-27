/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.Actor
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.Optional
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

  @Test
  fun `saved definition lookup returns only the definition in the requested workspace for both actor types`() {
    for (type in listOf(ActorType.source, ActorType.destination)) {
      val actor =
        actorRepository.save(
          Actor(
            workspaceId = UUID.randomUUID(),
            actorDefinitionId = UUID.randomUUID(),
            name = "saved-$type",
            configuration = Jsons.jsonNode(mapOf("password" to "stored-config")),
            actorType = type,
          ),
        )

      assertEquals(
        Optional.of(actor.actorDefinitionId),
        actorRepository.findActorDefinitionIdByIdAndWorkspaceId(actor.id!!, actor.workspaceId),
      )
      assertTrue(actorRepository.findActorDefinitionIdByIdAndWorkspaceId(actor.id!!, UUID.randomUUID()).isEmpty)
    }
  }

  @Test
  fun `saved definition lookup is empty for a missing actor`() {
    assertTrue(actorRepository.findActorDefinitionIdByIdAndWorkspaceId(UUID.randomUUID(), UUID.randomUUID()).isEmpty)
  }

  @Test
  fun `source ownership projection excludes destinations and accepts saved sources without reading configuration`() {
    for (type in listOf(ActorType.source, ActorType.destination)) {
      for (tombstone in listOf(false, true)) {
        val actor =
          actorRepository.save(
            Actor(
              workspaceId = UUID.randomUUID(),
              actorDefinitionId = UUID.randomUUID(),
              name = "saved-$type",
              configuration = Jsons.jsonNode(mapOf("password" to "stored-config")),
              actorType = type,
              tombstone = tombstone,
            ),
          )
        assertEquals(
          if (type ==
            ActorType.source
          ) {
            Optional.of(actor.workspaceId)
          } else {
            Optional.empty()
          },
          actorRepository.findSourceWorkspaceId(actor.id!!),
        )
      }
    }
    assertTrue(actorRepository.findSourceWorkspaceId(UUID.randomUUID()).isEmpty)
    assertTrue(actorRepository.findSourceWorkspaceId(UUID(0, 0)).isEmpty)
  }

  @Test
  fun `ordinary entity updates preserve independently written enablements`() {
    for (type in listOf(ActorType.source, ActorType.destination)) {
      val actor =
        actorRepository.save(
          Actor(
            workspaceId = UUID.randomUUID(),
            actorDefinitionId = UUID.randomUUID(),
            name = "before",
            configuration = Jsons.emptyObject(),
            actorType = type,
          ),
        )
      jooqDslContext.execute("UPDATE actor SET enable_agent_access = true, enable_indexing = true WHERE id = ?", actor.id)
      // Updating an entity read before enablement was written must preserve that later write.
      actor.name = "after"
      actor.configuration = Jsons.jsonNode(mapOf("edited" to true))
      actorRepository.update(actor)
      assertEquals("after", actorRepository.findByActorId(actor.id!!)!!.name)
      assertEquals(true, jooqDslContext.fetchValue("SELECT enable_agent_access AND enable_indexing FROM actor WHERE id = ?", actor.id))
    }
  }
}
