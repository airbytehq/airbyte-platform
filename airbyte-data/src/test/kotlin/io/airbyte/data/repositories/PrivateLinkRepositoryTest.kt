/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.PrivateLink
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.PrivateLinkStatus
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.jooq.impl.DSL
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class PrivateLinkRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    private val dataplaneGroupId = UUID.randomUUID()

    @BeforeAll
    @JvmStatic
    fun setup() {
      jooqDslContext
        .alterTable(Tables.PRIVATE_LINK)
        .dropForeignKey(Keys.PRIVATE_LINK__PRIVATE_LINK_WORKSPACE_ID_FKEY.constraint())
        .execute()
      jooqDslContext
        .alterTable(Tables.PRIVATE_LINK)
        .dropForeignKey(DSL.constraint("private_link_dataplane_group_id_fkey"))
        .execute()
      jooqDslContext
        .alterTable(DSL.table("dataplane_group"))
        .dropForeignKey(DSL.constraint("dataplane_group_organization_id_fkey"))
        .execute()
      jooqDslContext
        .insertInto(
          DSL.table("dataplane_group"),
          DSL.field("id"),
          DSL.field("organization_id"),
          DSL.field("name"),
        ).values(dataplaneGroupId, UUID.randomUUID(), "test-group")
        .execute()
    }
  }

  @AfterEach
  fun tearDown() {
    privateLinkRepository.deleteAll()
  }

  private fun createPrivateLink(
    workspaceId: UUID = UUID.randomUUID(),
    name: String = "test-link",
    serviceName: String = "com.amazonaws.vpce.us-east-1.vpce-svc-test",
  ): PrivateLink =
    privateLinkRepository.save(
      PrivateLink(
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = name,
        status = PrivateLinkStatus.creating,
        serviceRegion = "us-east-1",
        serviceName = serviceName,
      ),
    )

  @Test
  fun `test create and retrieve private link`() {
    val workspaceId = UUID.randomUUID()
    val saved = createPrivateLink(workspaceId = workspaceId)

    assertNotNull(saved.id)

    val retrieved = privateLinkRepository.findById(saved.id!!).get()
    assertEquals(workspaceId, retrieved.workspaceId)
    assertEquals(dataplaneGroupId, retrieved.dataplaneGroupId)
    assertEquals("test-link", retrieved.name)
    assertEquals(PrivateLinkStatus.creating, retrieved.status)
    assertEquals("us-east-1", retrieved.serviceRegion)
  }

  @Test
  fun `test create with custom name`() {
    val saved = createPrivateLink(name = "my-prod-link-01")

    val retrieved = privateLinkRepository.findById(saved.id!!).get()
    assertEquals("my-prod-link-01", retrieved.name)
  }

  @Test
  fun `test list by workspace id`() {
    val workspaceId = UUID.randomUUID()
    val otherWorkspaceId = UUID.randomUUID()

    createPrivateLink(workspaceId = workspaceId, name = "test-link-1", serviceName = "com.amazonaws.vpce.us-east-1.vpce-svc-1")
    createPrivateLink(workspaceId = workspaceId, name = "test-link-2", serviceName = "com.amazonaws.vpce.us-east-1.vpce-svc-2")
    createPrivateLink(workspaceId = otherWorkspaceId)

    val results = privateLinkRepository.findByWorkspaceId(workspaceId)
    assertEquals(2, results.size)
    assertTrue(results.all { it.workspaceId == workspaceId })
  }

  @Test
  fun `test list by workspace id returns empty when none exist`() {
    val results = privateLinkRepository.findByWorkspaceId(UUID.randomUUID())
    assertTrue(results.isEmpty())
  }

  @Test
  fun `test update status`() {
    val saved = createPrivateLink()

    saved.status = PrivateLinkStatus.available
    saved.endpointId = "vpce-0a1b2c3d4e5f6g7h8"
    val updated = privateLinkRepository.update(saved)

    assertEquals(PrivateLinkStatus.available, updated.status)
    assertEquals("vpce-0a1b2c3d4e5f6g7h8", updated.endpointId)
  }

  @Test
  fun `test status transitions for deletion lifecycle`() {
    val saved = createPrivateLink()

    saved.status = PrivateLinkStatus.available
    privateLinkRepository.update(saved)

    saved.status = PrivateLinkStatus.deleting
    val deleting = privateLinkRepository.update(saved)
    assertEquals(PrivateLinkStatus.deleting, deleting.status)

    saved.status = PrivateLinkStatus.delete_failed
    val failed = privateLinkRepository.update(saved)
    assertEquals(PrivateLinkStatus.delete_failed, failed.status)
  }

  @Test
  fun `test pending_acceptance status`() {
    val saved = createPrivateLink()

    saved.status = PrivateLinkStatus.pending_acceptance
    saved.endpointId = "vpce-0a1b2c3d4e5f6g7h8"
    val updated = privateLinkRepository.update(saved)

    assertEquals(PrivateLinkStatus.pending_acceptance, updated.status)
    assertEquals("vpce-0a1b2c3d4e5f6g7h8", updated.endpointId)
  }

  @Test
  fun `test create_failed status`() {
    val saved = createPrivateLink()

    saved.status = PrivateLinkStatus.create_failed
    val updated = privateLinkRepository.update(saved)

    assertEquals(PrivateLinkStatus.create_failed, updated.status)
  }

  @Test
  fun `test update dns name`() {
    val saved = createPrivateLink()

    saved.dnsName = "vpce-0a1b2c3d4e5f6g7h8-abcdef12.vpce-svc-test.us-east-1.vpce.amazonaws.com"
    val updated = privateLinkRepository.update(saved)

    assertEquals("vpce-0a1b2c3d4e5f6g7h8-abcdef12.vpce-svc-test.us-east-1.vpce.amazonaws.com", updated.dnsName)
  }

  @Test
  fun `test delete by id`() {
    val saved = createPrivateLink()
    privateLinkRepository.deleteById(saved.id!!)

    assertTrue(privateLinkRepository.findById(saved.id!!).isEmpty)
  }
}
