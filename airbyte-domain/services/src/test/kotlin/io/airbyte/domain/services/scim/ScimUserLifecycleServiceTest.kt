/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.ScimUserGroupMembershipRow
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.ScimAirbyteUser
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.airbyte.domain.models.scim.ScimUserConflictException
import io.airbyte.domain.models.scim.ScimUserFilterAttribute
import io.airbyte.domain.models.scim.ScimUserFilterClause
import io.airbyte.domain.models.scim.ScimUserNotFoundException
import io.airbyte.domain.models.scim.ScimUserWrite
import io.micronaut.data.exceptions.DataAccessException
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

class ScimUserLifecycleServiceTest {
  private val objectMapper = jacksonObjectMapper()
  private val mappingRepository = mockk<ScimResourceMappingRepository>()
  private val userRepository = mockk<ScimAirbyteUserRepository>()
  private val permissionRepository = mockk<PermissionRepository>()
  private val groupMemberRepository = mockk<GroupMemberRepository>()
  private val service =
    ScimUserLifecycleService(
      mappingRepository,
      userRepository,
      permissionRepository,
      groupMemberRepository,
    )
  private val configurationId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    every { mappingRepository.findGroupsForUser(configurationId, organizationId, any()) } returns emptyList()
    every { userRepository.acquireGlobalEmailLock(any()) } returns false
    every { userRepository.findByEmailIgnoreCaseForUpdate(any()) } returns emptyList()
    every { mappingRepository.findUserByUserId(any(), configurationId, organizationId) } returns null
  }

  @Test
  fun `POST rejects any scoped identifier match without writes`() {
    listOf(
      mapping(userName = "ALICE"),
      mapping(primaryEmail = "ALICE@EXAMPLE.COM"),
      mapping(externalId = "external"),
    ).forEach { existing ->
      every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(existing)

      assertThrows<ScimUserConflictException> {
        service.create(configurationId, organizationId, write())
      }
    }

    verify(exactly = 0) { userRepository.findByEmailIgnoreCaseForUpdate(any()) }
    verify(exactly = 0) { mappingRepository.save(any()) }
    verify(exactly = 0) { permissionRepository.save(any()) }
  }

  @Test
  fun `POST reuses one global User and preserves an existing elevated organization permission`() {
    val user = ScimAirbyteUser(id = UUID.randomUUID(), name = "Existing", email = "Alice@example.com")
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns emptyList()
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns listOf(user)
    every { mappingRepository.save(any()) } answers { saved(firstArg()) }
    every { permissionRepository.existsByUserIdAndOrganizationId(user.id, organizationId) } returns true

    val result = service.create(configurationId, organizationId, write())

    assertThat(result.userId).isEqualTo(user.id)
    assertThat(result.id).isNotNull()
    assertThat(result.active).isTrue()
    verify(exactly = 0) { userRepository.save(any()) }
    verify(exactly = 0) { permissionRepository.save(any()) }
  }

  @Test
  fun `POST creates a global User without auth identity and adds baseline access`() {
    val userSlot = slot<ScimAirbyteUser>()
    val permissionSlot = slot<Permission>()
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns emptyList()
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns emptyList()
    every { userRepository.save(capture(userSlot)) } answers { firstArg() }
    every { mappingRepository.save(any()) } answers { saved(firstArg()) }
    every { permissionRepository.existsByUserIdAndOrganizationId(any(), organizationId) } returns false
    every { permissionRepository.save(capture(permissionSlot)) } answers { firstArg() }

    val result = service.create(configurationId, organizationId, write())

    assertThat(userSlot.captured.email).isEqualTo("alice@example.com")
    assertThat(userSlot.captured.name).isEqualTo("Alice Example")
    assertThat(result.userId).isEqualTo(userSlot.captured.id)
    assertThat(permissionSlot.captured.userId).isEqualTo(result.userId)
    assertThat(permissionSlot.captured.organizationId).isEqualTo(organizationId)
    assertThat(permissionSlot.captured.permissionType).isEqualTo(PermissionType.organization_member)
    assertThat(permissionSlot.captured.workspaceId).isNull()
  }

  @Test
  fun `inactive POST removes target organization access for a reused global User`() {
    val user = ScimAirbyteUser(id = UUID.randomUUID(), name = "Existing", email = "Alice@example.com")
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns emptyList()
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns listOf(user)
    every { mappingRepository.save(any()) } answers { saved(firstArg()) }
    every { permissionRepository.deleteByUserIdAndOrganizationId(user.id, organizationId) } returns 1
    every { permissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(user.id, organizationId) } returns 2
    every { groupMemberRepository.deleteByUserIdAndOrganizationId(user.id, organizationId) } returns 3

    val result = service.create(configurationId, organizationId, write(active = false))

    assertThat(result.active).isFalse()
    verifyOrder {
      permissionRepository.deleteByUserIdAndOrganizationId(user.id, organizationId)
      permissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(user.id, organizationId)
      groupMemberRepository.deleteByUserIdAndOrganizationId(user.id, organizationId)
    }
  }

  @Test
  fun `POST rejects ambiguous global identity matches`() {
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns emptyList()
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns
      listOf(
        ScimAirbyteUser(UUID.randomUUID(), "First", "alice@example.com"),
        ScimAirbyteUser(UUID.randomUUID(), "Second", "ALICE@example.com"),
      )

    assertThrows<ScimUserConflictException> {
      service.create(configurationId, organizationId, write())
    }

    verify(exactly = 0) { mappingRepository.save(any()) }
  }

  @Test
  fun `POST serializes global identity resolution before lookup`() {
    val user = ScimAirbyteUser(id = UUID.randomUUID(), name = "Existing", email = "Alice@example.com")
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns emptyList()
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns listOf(user)
    every { mappingRepository.save(any()) } answers { saved(firstArg()) }
    every { permissionRepository.existsByUserIdAndOrganizationId(user.id, organizationId) } returns true

    service.create(configurationId, organizationId, write())

    verifyOrder {
      userRepository.acquireGlobalEmailLock("alice@example.com")
      userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com")
      mappingRepository.findUserByUserId(user.id, configurationId, organizationId)
      mappingRepository.save(any())
    }
  }

  @Test
  fun `POST rejects a resolved global User that already has a scoped mapping`() {
    val user = ScimAirbyteUser(id = UUID.randomUUID(), name = "Existing", email = "Alice@example.com")
    val staleMapping =
      mapping(userName = "stale", primaryEmail = "stale@example.com", externalId = "stale-external").copy(userId = user.id)
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(staleMapping)
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns listOf(user)
    every { mappingRepository.findUserByUserId(user.id, configurationId, organizationId) } returns staleMapping

    assertThrows<ScimUserConflictException> {
      service.create(configurationId, organizationId, write())
    }

    verify(exactly = 0) { mappingRepository.save(any()) }
    verify(exactly = 0) { permissionRepository.save(any()) }
  }

  @Test
  fun `POST translates mapping uniqueness races and propagates unrelated repository failures`() {
    val user = ScimAirbyteUser(id = UUID.randomUUID(), name = "Existing", email = "Alice@example.com")
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns emptyList()
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns listOf(user)
    every { mappingRepository.save(any()) } throws
      DataAccessException("duplicate key value violates unique constraint scim_resource_mapping_primary_email_key")

    assertThrows<ScimUserConflictException> {
      service.create(configurationId, organizationId, write())
    }

    val failure = DataAccessException("connection failed")
    every { mappingRepository.save(any()) } throws failure

    val thrown =
      assertThrows<DataAccessException> {
        service.create(configurationId, organizationId, write())
      }
    assertThat(thrown).isSameAs(failure)
  }

  @Test
  fun `PUT uses path mapping and rejects another mapping identifier before update`() {
    val target = mapping(userName = "target", primaryEmail = "target@example.com", externalId = "target-external")
    val conflict = mapping(userName = "alice")
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target, conflict)

    assertThrows<ScimUserConflictException> {
      service.replace(configurationId, organizationId, target.id!!, write())
    }

    verify(exactly = 0) { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PUT translates mapping uniqueness races and propagates unrelated repository failures`() {
    val target = mapping(userName = "target", primaryEmail = "target@example.com", externalId = "target-external")
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target)

    listOf(
      "scim_resource_mapping_external_id_key",
      "scim_resource_mapping_primary_email_key",
      "scim_resource_mapping_user_name_key",
    ).forEach { constraint ->
      every { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) } throws
        DataAccessException("duplicate key value violates unique constraint $constraint")

      assertThrows<ScimUserConflictException> {
        service.replace(configurationId, organizationId, target.id!!, write())
      }
    }

    val failure = DataAccessException("connection failed")
    every { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) } throws failure

    val thrown =
      assertThrows<DataAccessException> {
        service.replace(configurationId, organizationId, target.id!!, write())
      }
    assertThat(thrown).isSameAs(failure)
  }

  @Test
  fun `PUT rejects a selected email belonging to a different global User without writes`() {
    val target = mapping(userName = "target", primaryEmail = "target@example.com", externalId = "target-external")
    val conflictingUser = ScimAirbyteUser(UUID.randomUUID(), "Conflicting", "alice@example.com")
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target)
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns listOf(conflictingUser)
    every { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) } returns 1
    every { mappingRepository.findUser(target.id!!, configurationId, organizationId) } returns saved(target)

    assertThrows<ScimUserConflictException> {
      service.replace(configurationId, organizationId, target.id!!, write())
    }

    verifyOrder {
      userRepository.acquireGlobalEmailLock("alice@example.com")
      userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com")
    }
    verify(exactly = 0) { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PUT rejects ambiguous global Users without writes`() {
    val target = mapping(userName = "target", primaryEmail = "target@example.com", externalId = "target-external")
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target)
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns
      listOf(
        ScimAirbyteUser(target.userId!!, "Target", "alice@example.com"),
        ScimAirbyteUser(UUID.randomUUID(), "Conflicting", "ALICE@example.com"),
      )
    every { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) } returns 1
    every { mappingRepository.findUser(target.id!!, configurationId, organizationId) } returns saved(target)

    assertThrows<ScimUserConflictException> {
      service.replace(configurationId, organizationId, target.id!!, write())
    }

    verify(exactly = 0) { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PATCH rejects a selected email belonging to a different global User without writes`() {
    val target = mapping(userName = "target", primaryEmail = "target@example.com", externalId = "target-external")
    val conflictingUser = ScimAirbyteUser(UUID.randomUUID(), "Conflicting", "alice@example.com")
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target)
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns listOf(conflictingUser)
    every { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) } returns 1
    every { mappingRepository.findUser(target.id!!, configurationId, organizationId) } returns saved(target)

    assertThrows<ScimUserConflictException> {
      service.patch(configurationId, organizationId, target.id!!, write(), emptyList())
    }

    verifyOrder {
      userRepository.acquireGlobalEmailLock("alice@example.com")
      userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com")
    }
    verify(exactly = 0) { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PATCH rejects ambiguous global Users without writes`() {
    val target = mapping(userName = "target", primaryEmail = "target@example.com", externalId = "target-external")
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target)
    every { userRepository.findByEmailIgnoreCaseForUpdate("alice@example.com") } returns
      listOf(
        ScimAirbyteUser(target.userId!!, "Target", "alice@example.com"),
        ScimAirbyteUser(UUID.randomUUID(), "Conflicting", "ALICE@example.com"),
      )
    every { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) } returns 1
    every { mappingRepository.findUser(target.id!!, configurationId, organizationId) } returns saved(target)

    assertThrows<ScimUserConflictException> {
      service.patch(configurationId, organizationId, target.id!!, write(), emptyList())
    }

    verify(exactly = 0) { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PUT changes or clears externalId and compares conflicts case exactly`() {
    val target = mapping(userName = "target", primaryEmail = "target@example.com", externalId = "old")
    val other = mapping(userName = "other", primaryEmail = "other@example.com", externalId = "CaseSensitive")
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target, other)
    every { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) } returns 1
    every { mappingRepository.findUser(target.id!!, configurationId, organizationId) } returns saved(target)

    service.replace(configurationId, organizationId, target.id!!, write(externalId = "casesensitive"))
    service.replace(configurationId, organizationId, target.id!!, write(externalId = null))

    verify {
      mappingRepository.updateUser(
        target.id!!,
        configurationId,
        organizationId,
        "alice",
        "alice@example.com",
        "casesensitive",
        true,
        any(),
      )
    }
    verify {
      mappingRepository.updateUser(
        target.id!!,
        configurationId,
        organizationId,
        "alice",
        "alice@example.com",
        null,
        true,
        any(),
      )
    }

    assertThrows<ScimUserConflictException> {
      service.replace(configurationId, organizationId, target.id!!, write(externalId = "CaseSensitive"))
    }
  }

  @Test
  fun `PATCH applies active transitions sequentially and updates the profile once`() {
    val target = mapping(userActive = true)
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target)
    every {
      mappingRepository.updateUser(
        target.id!!,
        configurationId,
        organizationId,
        "alice",
        "alice@example.com",
        "external",
        true,
        any(),
      )
    } returns 1
    every { permissionRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 1
    every { permissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 2
    every { groupMemberRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 3
    every { permissionRepository.existsByUserIdAndOrganizationId(target.userId!!, organizationId) } returns false
    every { permissionRepository.save(any()) } answers { firstArg() }
    every { mappingRepository.findUser(target.id!!, configurationId, organizationId) } returns saved(target.copy(userActive = true))

    service.patch(configurationId, organizationId, target.id!!, write(active = true), listOf(false, true))

    verifyOrder {
      permissionRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId)
      permissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(target.userId!!, organizationId)
      groupMemberRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId)
      permissionRepository.existsByUserIdAndOrganizationId(target.userId!!, organizationId)
      permissionRepository.save(any())
    }
    verify(exactly = 1) { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PUT deactivation removes only target organization access and returns inactive mapping`() {
    val target = mapping(userActive = true)
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { mappingRepository.findAllUsers(configurationId, organizationId) } returns listOf(target)
    every { mappingRepository.updateUser(any(), any(), any(), any(), any(), any(), false, any()) } returns 1
    every { permissionRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 1
    every { permissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 1
    every { groupMemberRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 1
    every { mappingRepository.findUser(target.id!!, configurationId, organizationId) } returns saved(target.copy(userActive = false))

    val result = service.replace(configurationId, organizationId, target.id!!, write(active = false))

    assertThat(result.active).isFalse()
    verify { permissionRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId) }
    verify { permissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(target.userId!!, organizationId) }
    verify { groupMemberRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId) }
  }

  @Test
  fun `DELETE performs lifecycle cleanup and deletes only the scoped mapping`() {
    val target = mapping(userActive = false)
    every { mappingRepository.findUserForUpdate(target.id!!, configurationId, organizationId) } returns target
    every { permissionRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 0
    every { permissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 0
    every { groupMemberRepository.deleteByUserIdAndOrganizationId(target.userId!!, organizationId) } returns 0
    every { mappingRepository.deleteUser(target.id!!, configurationId, organizationId) } returns 1

    service.delete(configurationId, organizationId, target.id!!)

    verify { mappingRepository.deleteUser(target.id!!, configurationId, organizationId) }
    verify(exactly = 0) { userRepository.deleteById(any()) }
  }

  @Test
  fun `GET unknown or cross tenant mapping is not found`() {
    val id = UUID.randomUUID()
    every { mappingRepository.findUser(id, configurationId, organizationId) } returns null

    assertThrows<ScimUserNotFoundException> {
      service.get(configurationId, organizationId, id)
    }
  }

  @Test
  fun `list applies and clauses before pagination using exact externalId and case insensitive email fields`() {
    val first = mapping(userName = "Alice", primaryEmail = "Alice@Example.com", externalId = "Exact")
    every {
      mappingRepository.countUsers(
        configurationId,
        organizationId,
        "alice",
        "Exact",
        listOf("alice@example.com"),
        emptyList(),
      )
    } returns 5
    every {
      mappingRepository.findUsersPage(
        configurationId,
        organizationId,
        "alice",
        "Exact",
        listOf("alice@example.com"),
        emptyList(),
        3,
        1,
      )
    } returns listOf(first)

    val result =
      service.list(
        configurationId,
        organizationId,
        listOf(
          ScimUserFilterClause(ScimUserFilterAttribute.USER_NAME, "alice"),
          ScimUserFilterClause(ScimUserFilterAttribute.EMAIL, "alice@example.com"),
          ScimUserFilterClause(ScimUserFilterAttribute.EXTERNAL_ID, "Exact"),
        ),
        offset = 3,
        limit = 1,
      )

    assertThat(result.totalResults).isEqualTo(5)
    assertThat(result.resources).extracting<UUID> { it.id }.containsExactly(first.id)
    verify(exactly = 0) { mappingRepository.findGroupsForUser(any(), any(), any()) }
  }

  @Test
  fun `list retains distinct email clauses for repository filtering`() {
    val first = mapping(userName = "Alice", primaryEmail = "primary@example.com")
    val emails = listOf("primary@example.com", "alias@example.com")
    every { mappingRepository.countUsers(configurationId, organizationId, null, null, emails, emptyList()) } returns 1
    every { mappingRepository.findUsersPage(configurationId, organizationId, null, null, emails, emptyList(), 0, 1) } returns listOf(first)

    val result =
      service.list(
        configurationId,
        organizationId,
        listOf(
          ScimUserFilterClause(ScimUserFilterAttribute.EMAIL, "primary@example.com"),
          ScimUserFilterClause(ScimUserFilterAttribute.EMAIL, "alias@example.com"),
        ),
        offset = 0,
        limit = 1,
      )

    assertThat(result.totalResults).isEqualTo(1)
    assertThat(result.resources).extracting<UUID> { it.id }.containsExactly(first.id)
  }

  @Test
  fun `list retains distinct work email clauses for repository filtering`() {
    val first = mapping(userName = "Alice", primaryEmail = "primary@example.com")
    val workEmails = listOf("primary@example.com", "alias@example.com")
    every { mappingRepository.countUsers(configurationId, organizationId, null, null, emptyList(), workEmails) } returns 1
    every { mappingRepository.findUsersPage(configurationId, organizationId, null, null, emptyList(), workEmails, 0, 1) } returns listOf(first)

    val result =
      service.list(
        configurationId,
        organizationId,
        listOf(
          ScimUserFilterClause(ScimUserFilterAttribute.WORK_EMAIL, "primary@example.com"),
          ScimUserFilterClause(ScimUserFilterAttribute.WORK_EMAIL, "alias@example.com"),
        ),
        offset = 0,
        limit = 1,
      )

    assertThat(result.totalResults).isEqualTo(1)
    assertThat(result.resources).extracting<UUID> { it.id }.containsExactly(first.id)
  }

  @Test
  fun `list short circuits contradictory repeated scalar clauses`() {
    listOf(
      listOf(
        ScimUserFilterClause(ScimUserFilterAttribute.USER_NAME, "alice"),
        ScimUserFilterClause(ScimUserFilterAttribute.USER_NAME, "bob"),
      ),
      listOf(
        ScimUserFilterClause(ScimUserFilterAttribute.EXTERNAL_ID, "Exact"),
        ScimUserFilterClause(ScimUserFilterAttribute.EXTERNAL_ID, "Different"),
      ),
    ).forEach { filters ->
      val result = service.list(configurationId, organizationId, filters, offset = 0, limit = 1)

      assertThat(result.totalResults).isZero()
      assertThat(result.resources).isEmpty()
    }

    verify(exactly = 0) { mappingRepository.countUsers(any(), any(), any(), any(), any(), any()) }
    verify(exactly = 0) { mappingRepository.findUsersPage(any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `list page group enrichment uses one tenant scoped query for selected Users`() {
    val second = mapping(userName = "second", primaryEmail = "second@example.com")
    val groupId = UUID.randomUUID()
    every { mappingRepository.countUsers(configurationId, organizationId, null, null, emptyList(), emptyList()) } returns 2
    every { mappingRepository.findUsersPage(configurationId, organizationId, null, null, emptyList(), emptyList(), 1, 1) } returns listOf(second)
    every {
      mappingRepository.findGroupsForUsers(
        configurationId,
        organizationId,
        listOf(second.userId!!),
      )
    } returns listOf(ScimUserGroupMembershipRow(second.userId!!, groupId, "Engineering"))

    val listed = service.list(configurationId, organizationId, offset = 1, limit = 1)
    val enriched = service.enrichGroups(configurationId, organizationId, listed.resources)

    assertThat(listed.totalResults).isEqualTo(2)
    assertThat(enriched.single().groups).containsExactly(
      io.airbyte.domain.models.scim
        .ScimUserGroup(groupId, "Engineering"),
    )
    verify(exactly = 1) {
      mappingRepository.findGroupsForUsers(
        configurationId,
        organizationId,
        listOf(second.userId!!),
      )
    }
    verify(exactly = 0) { mappingRepository.findGroupsForUser(any(), any(), any()) }
  }

  private fun write(
    active: Boolean = true,
    externalId: String? = "external",
  ): ScimUserWrite =
    ScimUserWrite(
      userName = "alice",
      externalId = externalId,
      primaryEmail = "alice@example.com",
      active = active,
      attributes =
        json(
          """
          {
            "name":{"formatted":"Alice Example"},
            "displayName":"Alice Display",
            "emails":[
              {"value":"alice@example.com","type":"work"},
              {"value":"alias@example.com","type":"home"}
            ]
          }
          """.trimIndent(),
        ),
    )

  private fun mapping(
    userName: String = "existing",
    primaryEmail: String = "existing@example.com",
    externalId: String? = "existing-external",
    userActive: Boolean = true,
  ): ScimResourceMapping =
    ScimResourceMapping(
      id = UUID.randomUUID(),
      scimConfigurationId = configurationId,
      organizationId = organizationId,
      resourceType = ScimResourceType.USER,
      userId = UUID.randomUUID(),
      externalId = externalId,
      userName = userName,
      primaryEmail = primaryEmail,
      userActive = userActive,
      attributes = json("""{"emails":[{"value":"$primaryEmail","type":"work"}]}"""),
      createdAt = NOW,
      updatedAt = NOW,
    )

  private fun saved(mapping: ScimResourceMapping): ScimResourceMapping =
    mapping.apply {
      if (id == null) id = UUID.randomUUID()
      if (createdAt == null) createdAt = NOW
      updatedAt = NOW
    }

  private fun json(value: String): ObjectNode = objectMapper.readTree(value) as ObjectNode

  private companion object {
    val NOW: OffsetDateTime = OffsetDateTime.of(2026, 7, 17, 0, 0, 0, 0, ZoneOffset.UTC)
  }
}
