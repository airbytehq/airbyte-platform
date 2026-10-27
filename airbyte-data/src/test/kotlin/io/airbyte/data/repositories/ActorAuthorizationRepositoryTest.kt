/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.Workspace
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class ActorAuthorizationRepositoryTest : AbstractConfigRepositoryTest() {
  private val actors = context.getBean(ActorRepository::class.java)
  private val organization = organizationRepository.save(Organization(name = "actor owner", email = "owner@example.com")).id!!
  private val group =
    dataplaneGroupRepository
      .save(
        DataplaneGroup(organizationId = organization, name = "region-${UUID.randomUUID()}", enabled = true, tombstone = false),
      ).id!!
  private val workspace =
    workspaceRepository
      .save(
        Workspace(name = "actors", slug = UUID.randomUUID().toString(), dataplaneGroupId = group, organizationId = organization),
      ).id!!

  @Test
  fun `source and destination ownership queries cannot accept the other actor kind`() {
    val source = actor("source")
    val destination = actor("destination")
    assertEquals(workspace, actors.findSourceWorkspaceId(source.first).orElseThrow())
    assertEquals(workspace, actors.findDestinationWorkspaceId(destination.first).orElseThrow())
    assertTrue(actors.findSourceWorkspaceId(destination.first).isEmpty)
    assertTrue(actors.findDestinationWorkspaceId(source.first).isEmpty)
    assertTrue(actors.findDestinationWorkspaceId(UUID(0, 0)).isEmpty)
    assertTrue(actors.findSourceWorkspaceId(UUID(0, 0)).isEmpty)
    assertEquals(ActorType.source, actors.findDefinitionTypeById(source.second).orElseThrow())
    assertEquals(ActorType.destination, actors.findDefinitionTypeById(destination.second).orElseThrow())
    assertTrue(actors.findDefinitionTypeById(UUID(0, 0)).isEmpty)
  }

  @Test
  fun `catalog authorization permits each actual fetcher of a deduplicated catalog and excludes unrelated actors`() {
    val first = actor("source").first
    val second = actor("source").first
    val unrelated = actor("source").first
    val catalog = UUID.randomUUID()
    jooqDslContext.execute(
      "INSERT INTO actor_catalog (id, catalog, catalog_hash, created_at, catalog_type) VALUES (?, '{}'::jsonb, ?, CURRENT_TIMESTAMP, 'source_catalog')",
      catalog,
      UUID.randomUUID().toString().replace("-", ""),
    )
    for (actor in listOf(first, second)) {
      jooqDslContext.execute(
        "INSERT INTO actor_catalog_fetch_event (id, actor_id, actor_catalog_id, config_hash, actor_version, created_at, modified_at) VALUES (?, ?, ?, 'hash', '1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        UUID.randomUUID(),
        actor,
        catalog,
      )
    }
    assertTrue(actors.catalogWasFetchedByActor(catalog, first))
    assertTrue(actors.catalogWasFetchedByActor(catalog, second))
    assertFalse(actors.catalogWasFetchedByActor(catalog, unrelated))
    assertFalse(actors.catalogWasFetchedByActor(UUID(0, 0), first))

    val foreignOrganization = organizationRepository.save(Organization(name = "other owner", email = "other@example.com")).id!!
    val foreignWorkspace =
      workspaceRepository
        .save(
          Workspace(
            name = "other catalog fetcher",
            slug = UUID.randomUUID().toString(),
            dataplaneGroupId = group,
            organizationId = foreignOrganization,
          ),
        ).id!!
    jooqDslContext.execute("UPDATE actor SET workspace_id = ? WHERE id = ?", foreignWorkspace, second)
    jooqDslContext.execute(
      "INSERT INTO actor_catalog_fetch_event (id, actor_id, actor_catalog_id, config_hash, actor_version, created_at, modified_at) VALUES (?, ?, ?, 'hash', '1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
      UUID.randomUUID(),
      first,
      catalog,
    )
    assertEquals(setOf(workspace, foreignWorkspace), actors.findSourceWorkspaceIdsByCatalogId(catalog).toSet())
    assertEquals(2, actors.findSourceWorkspaceIdsByCatalogId(catalog).size)
    assertTrue(actors.findSourceWorkspaceIdsByCatalogId(UUID(0, 0)).isEmpty())

    val destination = actor("destination").first
    val destinationOnlyCatalog = UUID.randomUUID()
    jooqDslContext.execute(
      "INSERT INTO actor_catalog (id, catalog, catalog_hash, created_at, catalog_type) VALUES (?, '{}'::jsonb, ?, CURRENT_TIMESTAMP, 'destination_catalog')",
      destinationOnlyCatalog,
      UUID.randomUUID().toString().replace("-", ""),
    )
    jooqDslContext.execute(
      "INSERT INTO actor_catalog_fetch_event (id, actor_id, actor_catalog_id, config_hash, actor_version, created_at, modified_at) VALUES (?, ?, ?, 'hash', '1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
      UUID.randomUUID(),
      destination,
      destinationOnlyCatalog,
    )
    assertTrue(actors.findSourceWorkspaceIdsByCatalogId(destinationOnlyCatalog).isEmpty())
  }

  @Test
  fun `sync workload authorization binds every saved tenant actor and assigned account without a payload read`() {
    val source = actor("source").first
    val destination = actor("destination").first
    val connection = UUID.randomUUID()
    val account = UUID.randomUUID()
    val dataplane = UUID.randomUUID()
    val workload = UUID.randomUUID().toString()
    jooqDslContext.execute("INSERT INTO service_accounts (id, name, secret, managed) VALUES (?, 'account', 'test-secret', true)", account)
    jooqDslContext.execute(
      "INSERT INTO connection (id, namespace_definition, source_id, destination_id, name, catalog, status) VALUES (?, 'source', ?, ?, 'sync', '{}'::jsonb, 'active')",
      connection,
      source,
      destination,
    )
    jooqDslContext.execute(
      "INSERT INTO dataplane (id, dataplane_group_id, name, service_account_id) VALUES (?, ?, 'dataplane', ?)",
      dataplane,
      group,
      account,
    )
    jooqDslContext.execute(
      "INSERT INTO workload (id, dataplane_id, status, input_payload, log_path, type, dataplane_group, workspace_id, organization_id) VALUES (?, ?, 'claimed', 'unread payload', 'logs', 'sync', ?, ?, ?)",
      workload,
      dataplane.toString(),
      group.toString(),
      workspace,
      organization,
    )

    fun assigned(
      org: UUID = organization,
      ws: UUID = workspace,
      sync: UUID = connection,
      src: UUID = source,
      dst: UUID = destination,
      id: String = workload,
      region: UUID = group,
      subject: UUID = account,
    ): Boolean = actors.isAssignedSyncWorkload(org, ws, sync, src, dst, id, region, subject)
    assertTrue(assigned())
    // A customer workload can be assigned to a dataplane in an Airbyte-owned shared region.
    jooqDslContext.execute(
      "INSERT INTO organization (id, name, email) VALUES (?, 'shared owner', 'shared@example.com') ON CONFLICT (id) DO NOTHING",
      DEFAULT_ORGANIZATION_ID,
    )
    jooqDslContext.execute("UPDATE dataplane_group SET organization_id = ? WHERE id = ?", DEFAULT_ORGANIZATION_ID, group)
    assertEquals(DEFAULT_ORGANIZATION_ID, dataplaneGroupRepository.findById(group).orElseThrow().organizationId)
    assertTrue(organization != DEFAULT_ORGANIZATION_ID)
    assertTrue(assigned())
    assertFalse(assigned(org = UUID.randomUUID()))
    assertFalse(assigned(ws = UUID.randomUUID()))
    assertFalse(assigned(sync = UUID.randomUUID()))
    assertFalse(assigned(src = destination))
    assertFalse(assigned(dst = source))
    assertFalse(assigned(id = "unknown"))
    assertFalse(assigned(region = UUID.randomUUID()))
    assertFalse(assigned(subject = UUID.randomUUID()))
    for (status in listOf("launched", "running")) {
      jooqDslContext.execute("UPDATE workload SET status = ?::workload_status WHERE id = ?", status, workload)
      assertTrue(assigned())
    }
    jooqDslContext.execute("UPDATE workload SET status = 'success' WHERE id = ?", workload)
    assertFalse(assigned())
  }

  private fun actor(kind: String): Pair<UUID, UUID> {
    val id = UUID.randomUUID()
    val definition = UUID.randomUUID()
    jooqDslContext.execute("INSERT INTO actor_definition (id, name, actor_type) VALUES (?, 'actor', ?::actor_type)", definition, kind)
    jooqDslContext.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'actor', '{}'::jsonb, ?::actor_type)",
      id,
      workspace,
      definition,
      kind,
    )
    return id to definition
  }
}
