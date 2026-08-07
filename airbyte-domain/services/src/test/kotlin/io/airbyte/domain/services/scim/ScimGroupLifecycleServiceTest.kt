/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.data.repositories.ScimActiveUserRow
import io.airbyte.data.repositories.ScimGroupMemberRow
import io.airbyte.data.repositories.ScimGroupPageMemberRow
import io.airbyte.data.repositories.ScimGroupRow
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.Group
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimGroupConflictException
import io.airbyte.domain.models.scim.ScimGroupFilterAttribute
import io.airbyte.domain.models.scim.ScimGroupFilterClause
import io.airbyte.domain.models.scim.ScimGroupInvalidMemberException
import io.airbyte.domain.models.scim.ScimGroupNotFoundException
import io.airbyte.domain.models.scim.ScimGroupWrite
import io.micronaut.data.exceptions.DataAccessException
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class ScimGroupLifecycleServiceTest {
  private val mappingRepository = mockk<ScimResourceMappingRepository>()
  private val groupService = mockk<GroupService>()
  private val service = ScimGroupLifecycleService(mappingRepository, groupService)
  private val configurationId = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val organizationId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val mappingId = UUID.fromString("33333333-3333-3333-3333-333333333333")
  private val groupId = UUID.fromString("44444444-4444-4444-4444-444444444444")
  private val userMappingId = UUID.fromString("55555555-5555-5555-5555-555555555555")
  private val userId = UUID.fromString("66666666-6666-6666-6666-666666666666")
  private val timestamp = OffsetDateTime.parse("2026-07-17T00:00:00Z")

  @BeforeEach
  fun setUp() {
    every { mappingRepository.findActiveUsersByIds(configurationId, organizationId, any()) } answers {
      thirdArg<Collection<UUID>>().map { ScimActiveUserRow(it, userId) }
    }
    every { groupService.replaceGroupMembersForScim(any(), any(), any(), any()) } returns Unit
  }

  @Test
  fun `create stores a new Group mapping and direct memberships then returns mapped members`() {
    val savedGroup = Group(groupId, "Engineering", null, organizationId, timestamp, timestamp)
    val savedMapping = mapping()
    every {
      groupService.createGroupForScim(configurationId, OrganizationId(organizationId), "Engineering")
    } returns savedGroup.toConfigModel()
    every { mappingRepository.save(any()) } returns savedMapping
    every { mappingRepository.findGroup(mappingId, configurationId, organizationId) } returns row()
    every { mappingRepository.findGroupMembers(configurationId, organizationId, groupId) } returns
      listOf(ScimGroupMemberRow(userMappingId, userId, "Alice Example"))

    val created = service.create(configurationId, organizationId, input())

    assertThat(created.id).isEqualTo(mappingId)
    assertThat(created.groupId).isEqualTo(groupId)
    assertThat(created.displayName).isEqualTo("Engineering")
    assertThat(created.members.single().id).isEqualTo(userMappingId)
    verify { groupService.createGroupForScim(configurationId, OrganizationId(organizationId), "Engineering") }
    val mapping = slot<ScimResourceMapping>()
    verify { mappingRepository.save(capture(mapping)) }
    assertThat(mapping.captured.resourceType).isEqualTo(ScimResourceType.GROUP)
    assertThat(mapping.captured.organizationId).isEqualTo(organizationId)
    assertThat(mapping.captured.attributes).isEqualTo(JsonNodeFactory.instance.objectNode())
    verify {
      groupService.replaceGroupMembersForScim(
        configurationId,
        OrganizationId(organizationId),
        GroupId(groupId),
        listOf(UserId(userId)),
      )
    }
  }

  @Test
  fun `create rejects any unresolved inactive nested or cross-tenant member before writing`() {
    val unknown = UUID.randomUUID()
    every { mappingRepository.findActiveUsersByIds(configurationId, organizationId, listOf(userMappingId, unknown)) } returns
      listOf(ScimActiveUserRow(userMappingId, userId))

    assertThrows<ScimGroupInvalidMemberException> {
      service.create(configurationId, organizationId, input(listOf(userMappingId, unknown)))
    }

    verify(exactly = 0) { groupService.createGroupForScim(any(), any(), any()) }
    verify(exactly = 0) { mappingRepository.save(any()) }
    verify(exactly = 0) { groupService.replaceGroupMembersForScim(any(), any(), any(), any()) }
  }

  @Test
  fun `create rejects a case-insensitive mapped or unmapped Group name collision`() {
    every {
      groupService.createGroupForScim(configurationId, OrganizationId(organizationId), "Engineering")
    } throws GroupNameNotUniqueException("collision")

    assertThrows<ScimGroupConflictException> { service.create(configurationId, organizationId, input(emptyList())) }

    verify(exactly = 0) { mappingRepository.save(any()) }
  }

  @Test
  fun `create translates an inactive member detected during bulk replacement`() {
    every {
      groupService.createGroupForScim(configurationId, OrganizationId(organizationId), "Engineering")
    } returns Group(groupId, "Engineering", null, organizationId, timestamp, timestamp).toConfigModel()
    every { mappingRepository.save(any()) } returns mapping()
    every {
      groupService.replaceGroupMembersForScim(
        configurationId,
        OrganizationId(organizationId),
        GroupId(groupId),
        listOf(UserId(userId)),
      )
    } throws InactiveUserAccessException("member became inactive")

    assertThrows<ScimGroupInvalidMemberException> {
      service.create(configurationId, organizationId, input(listOf(userMappingId)))
    }
  }

  @Test
  fun `replace validates all members before changing Group mapping or membership`() {
    val unknown = UUID.randomUUID()
    every { mappingRepository.findGroupForUpdate(mappingId, configurationId, organizationId) } returns mapping()
    every { mappingRepository.findActiveUsersByIds(configurationId, organizationId, listOf(unknown)) } returns emptyList()

    assertThrows<ScimGroupInvalidMemberException> {
      service.replace(configurationId, organizationId, mappingId, input(listOf(unknown)))
    }

    verify(exactly = 0) { groupService.updateGroupForScim(any(), any(), any(), any()) }
    verify(exactly = 0) { mappingRepository.updateGroup(any(), any(), any(), any()) }
    verify(exactly = 0) { groupService.replaceGroupMembersForScim(any(), any(), any(), any()) }
  }

  @Test
  fun `replace changes name externalId and the complete membership set`() {
    every { mappingRepository.findGroupForUpdate(mappingId, configurationId, organizationId) } returns mapping()
    every {
      groupService.updateGroupForScim(configurationId, OrganizationId(organizationId), GroupId(groupId), "Engineering")
    } returns Group(groupId, "Engineering", null, organizationId, timestamp, timestamp).toConfigModel()
    every { mappingRepository.updateGroup(mappingId, configurationId, organizationId, "Case-Exact") } returns 1L
    every { mappingRepository.findGroup(mappingId, configurationId, organizationId) } returns row()
    every { mappingRepository.findGroupMembers(configurationId, organizationId, groupId) } returns
      listOf(ScimGroupMemberRow(userMappingId, userId, "Alice Example"))

    val updated = service.replace(configurationId, organizationId, mappingId, input())

    assertThat(updated.externalId).isEqualTo("Case-Exact")
    assertThat(updated.members.map { it.id }).containsExactly(userMappingId)
    verify {
      groupService.replaceGroupMembersForScim(
        configurationId,
        OrganizationId(organizationId),
        GroupId(groupId),
        listOf(UserId(userId)),
      )
    }
  }

  @Test
  fun `list filters mapped Groups by case-insensitive displayName or exact member mapping id`() {
    every { mappingRepository.countGroups(configurationId, organizationId, "engineering", null) } returns 1
    every { mappingRepository.findGroupsPage(configurationId, organizationId, "engineering", null, 0, 100) } returns listOf(row())
    every { mappingRepository.countGroups(configurationId, organizationId, null, userMappingId.toString()) } returns 1
    every { mappingRepository.findGroupsPage(configurationId, organizationId, null, userMappingId.toString(), 0, 100) } returns listOf(row())
    every { mappingRepository.findGroupMembersForGroups(configurationId, organizationId, listOf(groupId)) } returns
      listOf(ScimGroupPageMemberRow(groupId, userMappingId, userId, "Alice Example"))

    val byName =
      service.list(
        configurationId,
        organizationId,
        ScimGroupFilterClause(ScimGroupFilterAttribute.DISPLAY_NAME, "engineering"),
        offset = 0,
        limit = 100,
      )
    val byMember =
      service.list(
        configurationId,
        organizationId,
        ScimGroupFilterClause(ScimGroupFilterAttribute.MEMBER, userMappingId.toString()),
        offset = 0,
        limit = 100,
      )

    assertThat(byName.resources.map { it.displayName }).containsExactly("Engineering")
    assertThat(byMember.resources.map { it.displayName }).containsExactly("Engineering")
  }

  @Test
  fun `list pages in the repository and batch loads members only for selected Groups`() {
    val other = row(id = UUID.randomUUID(), groupId = UUID.randomUUID(), displayName = "Finance")
    every { mappingRepository.countGroups(configurationId, organizationId, null, null) } returns 2
    every { mappingRepository.findGroupsPage(configurationId, organizationId, null, null, 1, 1) } returns listOf(other)
    every { mappingRepository.findGroupMembersForGroups(configurationId, organizationId, listOf(other.groupId)) } returns emptyList()

    val page = service.list(configurationId, organizationId, null, offset = 1, limit = 1)

    assertThat(page.totalResults).isEqualTo(2)
    assertThat(page.resources.map { it.displayName }).containsExactly("Finance")
    verify(exactly = 1) { mappingRepository.findGroupMembersForGroups(configurationId, organizationId, listOf(other.groupId)) }
    verify(exactly = 0) { mappingRepository.findGroupMembers(any(), any(), any()) }
  }

  @Test
  fun `list preserves totals without loading a page or members when count is zero`() {
    every { mappingRepository.countGroups(configurationId, organizationId, null, null) } returns 2

    val page = service.list(configurationId, organizationId, null, offset = 0, limit = 0)

    assertThat(page.totalResults).isEqualTo(2)
    assertThat(page.resources).isEmpty()
    verify(exactly = 0) { mappingRepository.findGroupsPage(any(), any(), any(), any(), any(), any()) }
    verify(exactly = 0) { mappingRepository.findGroupMembersForGroups(any(), any(), any()) }
  }

  @Test
  fun `delete removes memberships and mapping but never deletes the underlying Group`() {
    every { mappingRepository.findGroupForUpdate(mappingId, configurationId, organizationId) } returns mapping()
    every {
      groupService.replaceGroupMembersForScim(configurationId, OrganizationId(organizationId), GroupId(groupId), emptyList())
    } returns Unit
    every { mappingRepository.deleteGroup(mappingId, configurationId, organizationId) } returns 1L

    service.delete(configurationId, organizationId, mappingId)

    verify {
      groupService.replaceGroupMembersForScim(configurationId, OrganizationId(organizationId), GroupId(groupId), emptyList())
    }
    verify { mappingRepository.deleteGroup(mappingId, configurationId, organizationId) }
  }

  @Test
  fun `deleted or unknown mapping ids return not found`() {
    every { mappingRepository.findGroupForUpdate(mappingId, configurationId, organizationId) } returns null

    assertThrows<ScimGroupNotFoundException> { service.delete(configurationId, organizationId, mappingId) }
  }

  @Test
  fun `active member validation accepts empty and complete sets only`() {
    val unknown = UUID.randomUUID()
    every { mappingRepository.findActiveUsersByIds(configurationId, organizationId, setOf(userMappingId, unknown)) } returns
      listOf(ScimActiveUserRow(userMappingId, userId))

    assertThat(service.areActiveUsers(configurationId, organizationId, emptySet())).isTrue
    assertThat(service.areActiveUsers(configurationId, organizationId, setOf(userMappingId))).isTrue
    assertThat(service.areActiveUsers(configurationId, organizationId, setOf(userMappingId, unknown))).isFalse
  }

  @Test
  fun `database uniqueness errors become Group conflicts and unrelated failures propagate`() {
    every {
      groupService.createGroupForScim(configurationId, OrganizationId(organizationId), "Engineering")
    } returns Group(groupId, "Engineering", null, organizationId, timestamp, timestamp).toConfigModel()
    every { mappingRepository.save(any()) } throws
      DataAccessException("duplicate scim_resource_mapping_external_id_key")

    assertThrows<ScimGroupConflictException> { service.create(configurationId, organizationId, input(emptyList())) }

    val failure = DataAccessException("connection lost")
    every { mappingRepository.save(any()) } throws failure
    assertThat(assertThrows<DataAccessException> { service.create(configurationId, organizationId, input(emptyList())) }).isSameAs(failure)
  }

  private fun input(memberIds: List<UUID> = listOf(userMappingId)): ScimGroupWrite =
    ScimGroupWrite(
      displayName = "Engineering",
      externalId = "Case-Exact",
      memberIds = memberIds,
    )

  private fun mapping(): ScimResourceMapping =
    ScimResourceMapping(
      id = mappingId,
      scimConfigurationId = configurationId,
      organizationId = organizationId,
      resourceType = ScimResourceType.GROUP,
      groupId = groupId,
      externalId = "Case-Exact",
      attributes = JsonNodeFactory.instance.objectNode(),
      createdAt = timestamp,
      updatedAt = timestamp,
    )

  private fun row(
    id: UUID = mappingId,
    groupId: UUID = this.groupId,
    displayName: String = "Engineering",
  ): ScimGroupRow =
    ScimGroupRow(
      id = id,
      scimConfigurationId = configurationId,
      organizationId = organizationId,
      groupId = groupId,
      externalId = "Case-Exact",
      displayName = displayName,
      createdAt = timestamp,
      updatedAt = timestamp,
    )

  private fun Group.toConfigModel(): io.airbyte.config.Group =
    io.airbyte.config.Group(
      groupId = GroupId(requireNotNull(id)),
      name = name,
      description = description,
      organizationId = OrganizationId(organizationId),
      memberCount = 0,
      createdAt = requireNotNull(createdAt),
      updatedAt = requireNotNull(updatedAt),
    )
}
