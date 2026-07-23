/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.Group
import io.airbyte.data.repositories.entities.GroupMember
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.ScimAirbyteUser
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.data.services.GroupManagedByScimException
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimGroupConflictException
import io.airbyte.domain.models.scim.ScimGroupFilterAttribute
import io.airbyte.domain.models.scim.ScimGroupFilterClause
import io.airbyte.domain.models.scim.ScimGroupInvalidMemberException
import io.airbyte.domain.models.scim.ScimGroupNotFoundException
import io.airbyte.domain.models.scim.ScimGroupWrite
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimGroupLifecycleService
import io.airbyte.domain.services.scim.ScimMutationService
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.transaction.TransactionOperations
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.net.URI
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import javax.sql.DataSource

class ScimGroupLifecycleIntegrationTest {
  @AfterEach
  fun cleanUp() {
    jooq.deleteFrom(Tables.GROUP_MEMBER).execute()
    jooq.deleteFrom(Tables.PERMISSION).execute()
    jooq.deleteFrom(Tables.SCIM_RESOURCE_MAPPING).execute()
    jooq.deleteFrom(Tables.GROUP).execute()
    jooq.deleteFrom(Tables.SCIM_CONFIGURATION).execute()
    jooq.deleteFrom(Tables.ORGANIZATION).execute()
    jooq.deleteFrom(Tables.USER).execute()
  }

  @Test
  fun `create list replace and delete persist the complete Group lifecycle without deleting the Airbyte Group`() {
    val tenant = tenant("lifecycle")
    val member = userMapping(tenant, "alice@example.com", active = true)
    val fallbackDisplayMember = userMapping(tenant, "fallback@example.com", active = true, displayName = null)
    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(
          tenant.configurationId,
          tenant.organizationId,
          input("Engineering", "external-1", listOf(member.id, fallbackDisplayMember.id)),
        )
      }

    assertThat(created.displayName).isEqualTo("Engineering")
    assertThat(created.externalId).isEqualTo("external-1")
    assertThat(created.members.map { it.id }).containsExactly(member.id, fallbackDisplayMember.id)
    assertThat(created.members.single { it.id == member.id }.display).isEqualTo("Alice Example")
    assertThat(created.members.single { it.id == fallbackDisplayMember.id }.display).isEqualTo("fallback@example.com")
    assertThat(created.createdAt).isEqualTo(created.updatedAt)
    assertThat(
      lifecycleService
        .list(
          tenant.configurationId,
          tenant.organizationId,
          ScimGroupFilterClause(ScimGroupFilterAttribute.MEMBER, member.id.toString()),
          offset = 0,
          limit = 100,
        ).resources
        .map { it.id },
    ).containsExactly(created.id)

    permissionRepository.save(
      Permission(
        groupId = created.groupId,
        organizationId = tenant.organizationId,
        permissionType = PermissionType.organization_reader,
      ),
    )
    val externalIdUpdated =
      mutationService.execute(tenant.context) {
        lifecycleService.replace(
          tenant.configurationId,
          tenant.organizationId,
          created.id,
          input("Platform", "replacement-External-ID", emptyList()),
        )
      }
    assertThat(externalIdUpdated.externalId).isEqualTo("replacement-External-ID")
    assertThat(lifecycleService.get(tenant.configurationId, tenant.organizationId, created.id).externalId)
      .isEqualTo("replacement-External-ID")

    val replaced =
      mutationService.execute(tenant.context) {
        lifecycleService.replace(tenant.configurationId, tenant.organizationId, created.id, input("Platform", null, emptyList()))
      }
    assertThat(replaced.displayName).isEqualTo("Platform")
    assertThat(replaced.externalId).isNull()
    assertThat(replaced.members).isEmpty()
    assertThat(replaced.updatedAt).isAfterOrEqualTo(created.updatedAt)

    mutationService.execute(tenant.context) {
      lifecycleService.delete(tenant.configurationId, tenant.organizationId, created.id)
    }
    assertThatThrownBy { lifecycleService.get(tenant.configurationId, tenant.organizationId, created.id) }
      .isInstanceOf(ScimGroupNotFoundException::class.java)
    assertThat(groupRepository.findById(created.groupId)).isPresent
    assertThat(groupRepository.findById(created.groupId).orElseThrow().name).isEqualTo("Platform")
    assertThat(permissionRepository.findByGroupId(created.groupId)).hasSize(1)
    assertThat(groupMemberRepository.countByGroupId(created.groupId)).isZero()
  }

  @Test
  fun `list filters and paginates in PostgreSQL before batch member loading`() {
    val tenant = tenant("list-page")
    val member = userMapping(tenant, "member@example.com", active = true)
    listOf("Engineering", "Finance", "Product").forEach { displayName ->
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input(displayName, null, emptyList()))
      }
    }
    val ordered = lifecycleService.list(tenant.configurationId, tenant.organizationId, null, offset = 0, limit = 10).resources
    val selected = ordered[1]
    groupMemberRepository.save(GroupMember(groupId = selected.groupId, userId = member.userId))

    val otherTenant = tenant("list-page-other")
    val otherMember = userMapping(otherTenant, "other-member@example.com", active = true)
    mutationService.execute(otherTenant.context) {
      lifecycleService.create(
        otherTenant.configurationId,
        otherTenant.organizationId,
        input(selected.displayName, null, listOf(otherMember.id)),
      )
    }

    val page = lifecycleService.list(tenant.configurationId, tenant.organizationId, null, offset = 1, limit = 1)
    val byDisplayName =
      lifecycleService.list(
        tenant.configurationId,
        tenant.organizationId,
        ScimGroupFilterClause(ScimGroupFilterAttribute.DISPLAY_NAME, selected.displayName.uppercase()),
        offset = 0,
        limit = 1,
      )
    val byMember =
      lifecycleService.list(
        tenant.configurationId,
        tenant.organizationId,
        ScimGroupFilterClause(ScimGroupFilterAttribute.MEMBER, member.id.toString()),
        offset = 0,
        limit = 1,
      )
    val invalidMember =
      lifecycleService.list(
        tenant.configurationId,
        tenant.organizationId,
        ScimGroupFilterClause(ScimGroupFilterAttribute.MEMBER, "not-a-uuid"),
        offset = 0,
        limit = 1,
      )
    val zeroCount = lifecycleService.list(tenant.configurationId, tenant.organizationId, null, offset = 0, limit = 0)

    assertThat(page.totalResults).isEqualTo(3)
    assertThat(page.resources.map { it.id }).containsExactly(selected.id)
    assertThat(
      page.resources
        .single()
        .members
        .map { it.id },
    ).containsExactly(member.id)
    assertThat(byDisplayName.totalResults).isEqualTo(1)
    assertThat(byDisplayName.resources.map { it.id }).containsExactly(selected.id)
    assertThat(byMember.totalResults).isEqualTo(1)
    assertThat(byMember.resources.map { it.id }).containsExactly(selected.id)
    assertThat(invalidMember.totalResults).isZero()
    assertThat(invalidMember.resources).isEmpty()
    assertThat(zeroCount.totalResults).isEqualTo(3)
    assertThat(zeroCount.resources).isEmpty()
  }

  @Test
  fun `member references are limited to active User mappings in the same organization and configuration before writes`() {
    val tenant = tenant("target")
    val otherOrganization = tenant("other-organization")
    val otherConfiguration = tenant("other-configuration")
    val inactive = userMapping(tenant, "inactive@example.com", active = false)
    val crossOrganization = userMapping(otherOrganization, "other-org@example.com", active = true)
    val crossConfiguration = userMapping(otherConfiguration, "other-config@example.com", active = true)
    val nestedGroup = groupRepository.save(Group(name = "Nested", organizationId = tenant.organizationId))
    val nestedMapping =
      mappingRepository.save(
        ScimResourceMapping(
          scimConfigurationId = tenant.configurationId,
          organizationId = tenant.organizationId,
          resourceType = ScimResourceType.GROUP,
          groupId = nestedGroup.id,
          externalId = "nested",
          attributes = objectMapper.createObjectNode(),
        ),
      )
    val nestedMappingId = requireNotNull(nestedMapping.id)
    assertThat(lifecycleService.get(tenant.configurationId, tenant.organizationId, nestedMappingId).displayName).isEqualTo("Nested")
    assertThatThrownBy {
      lifecycleService.get(otherOrganization.configurationId, otherOrganization.organizationId, nestedMappingId)
    }.isInstanceOf(ScimGroupNotFoundException::class.java)
    assertThat(
      lifecycleService.list(otherOrganization.configurationId, otherOrganization.organizationId, null, offset = 0, limit = 100).resources,
    ).isEmpty()
    val groupsBefore = jooq.fetchCount(Tables.GROUP)
    val mappingsBefore = jooq.fetchCount(Tables.SCIM_RESOURCE_MAPPING)

    listOf(inactive.id, crossOrganization.id, crossConfiguration.id, nestedMappingId, UUID.randomUUID()).forEach { memberId ->
      assertThatThrownBy {
        mutationService.execute(tenant.context) {
          lifecycleService.create(tenant.configurationId, tenant.organizationId, input("Rejected $memberId", null, listOf(memberId)))
        }
      }.isInstanceOf(ScimGroupInvalidMemberException::class.java)
    }

    assertThat(jooq.fetchCount(Tables.GROUP)).isEqualTo(groupsBefore)
    assertThat(jooq.fetchCount(Tables.SCIM_RESOURCE_MAPPING)).isEqualTo(mappingsBefore)
    assertThat(jooq.fetchCount(Tables.GROUP_MEMBER)).isZero()
  }

  @Test
  fun `active mapped User can join a SCIM Group after direct organization permission is deleted`() {
    val tenant = tenant("active-mapping-without-direct-permission")
    val member = userMapping(tenant, "mapped@example.com", active = true)
    jooq
      .deleteFrom(Tables.PERMISSION)
      .where(Tables.PERMISSION.USER_ID.eq(member.userId))
      .and(Tables.PERMISSION.ORGANIZATION_ID.eq(tenant.organizationId))
      .execute()

    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(
          tenant.configurationId,
          tenant.organizationId,
          input("Engineering", null, listOf(member.id)),
        )
      }

    assertThat(created.members.map { it.id }).containsExactly(member.id)
    assertThat(permissionRepository.findByUserId(member.userId)).isEmpty()
  }

  @Test
  fun `SCIM bulk replacement rejects a member that becomes inactive after initial resolution`() {
    val tenant = tenant("inactive-during-bulk-replacement")
    val member = userMapping(tenant, "race@example.com", active = true)
    val racingGroupService = mockk<GroupService>()
    every {
      racingGroupService.createGroupForScim(
        tenant.configurationId,
        OrganizationId(tenant.organizationId),
        "Engineering",
      )
    } answers {
      groupService.createGroupForScim(
        tenant.configurationId,
        OrganizationId(tenant.organizationId),
        "Engineering",
      )
    }
    every {
      racingGroupService.replaceGroupMembersForScim(
        tenant.configurationId,
        OrganizationId(tenant.organizationId),
        any(),
        listOf(UserId(member.userId)),
      )
    } answers {
      assertThat(
        jooq
          .update(Tables.SCIM_RESOURCE_MAPPING)
          .set(Tables.SCIM_RESOURCE_MAPPING.USER_ACTIVE, false)
          .where(Tables.SCIM_RESOURCE_MAPPING.ID.eq(member.id))
          .and(Tables.SCIM_RESOURCE_MAPPING.SCIM_CONFIGURATION_ID.eq(tenant.configurationId))
          .and(Tables.SCIM_RESOURCE_MAPPING.ORGANIZATION_ID.eq(tenant.organizationId))
          .execute(),
      ).isEqualTo(1)
      groupService.replaceGroupMembersForScim(
        tenant.configurationId,
        OrganizationId(tenant.organizationId),
        GroupId(thirdArg<UUID>()),
        listOf(UserId(member.userId)),
      )
    }
    val racingLifecycleService = ScimGroupLifecycleService(mappingRepository, racingGroupService)

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        racingLifecycleService.create(
          tenant.configurationId,
          tenant.organizationId,
          input("Engineering", null, listOf(member.id)),
        )
      }
    }.isInstanceOf(ScimGroupInvalidMemberException::class.java)
  }

  @Test
  fun `database uniqueness conflicts preserve the existing Group and mapping`() {
    val tenant = tenant("uniqueness")
    val first =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("Engineering", "shared-external", emptyList()))
      }
    val second =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("Product", "product-external", emptyList()))
      }
    val manual = groupService.createGroup(airbyteGroup(tenant.organizationId, "Manual"))

    assertThatThrownBy {
      groupService.updateGroup(manual.copy(name = "ENGINEERING"))
    }.isInstanceOf(GroupNameNotUniqueException::class.java)
    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        lifecycleService.replace(
          tenant.configurationId,
          tenant.organizationId,
          second.id,
          input("MANUAL", second.externalId, emptyList()),
        )
      }
    }.isInstanceOf(ScimGroupConflictException::class.java)

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        lifecycleService.replace(tenant.configurationId, tenant.organizationId, second.id, input("Platform", first.externalId, emptyList()))
      }
    }.isInstanceOf(ScimGroupConflictException::class.java)

    val unchanged = lifecycleService.get(tenant.configurationId, tenant.organizationId, second.id)
    assertThat(unchanged.displayName).isEqualTo("Product")
    assertThat(unchanged.externalId).isEqualTo("product-external")
    groupRepository.save(Group(name = "case collision", organizationId = tenant.organizationId))
    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("CASE COLLISION", null, emptyList()))
      }
    }.isInstanceOf(ScimGroupConflictException::class.java)
  }

  @Test
  fun `repository failures roll back name mapping and membership replacement and propagate unchanged`() {
    val tenant = tenant("rollback")
    val originalMember = userMapping(tenant, "original@example.com", active = true)
    val replacementMember = userMapping(tenant, "replacement@example.com", active = true)
    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("Engineering", "original-external", listOf(originalMember.id)))
      }
    val failure = ExpectedFailure()
    val failingGroupService = mockk<GroupService>()
    every {
      failingGroupService.updateGroupForScim(
        tenant.configurationId,
        OrganizationId(tenant.organizationId),
        GroupId(created.groupId),
        "Platform",
      )
    } answers {
      groupService.updateGroupForScim(
        tenant.configurationId,
        OrganizationId(tenant.organizationId),
        GroupId(created.groupId),
        "Platform",
      )
    }
    every {
      failingGroupService.replaceGroupMembersForScim(
        tenant.configurationId,
        OrganizationId(tenant.organizationId),
        GroupId(created.groupId),
        listOf(UserId(replacementMember.userId)),
      )
    } throws failure
    val failingLifecycle = ScimGroupLifecycleService(mappingRepository, failingGroupService)

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        failingLifecycle.replace(
          tenant.configurationId,
          tenant.organizationId,
          created.id,
          input("Platform", "updated-external", listOf(replacementMember.id)),
        )
      }
    }.isSameAs(failure)

    val restored = lifecycleService.get(tenant.configurationId, tenant.organizationId, created.id)
    assertThat(restored.displayName).isEqualTo("Engineering")
    assertThat(restored.externalId).isEqualTo("original-external")
    assertThat(restored.members.map { it.id }).containsExactly(originalMember.id)
  }

  @Test
  fun `failed create rolls back Group mapping and membership together`() {
    val tenant = tenant("create-rollback")
    val member = userMapping(tenant, "rollback@example.com", active = true)
    val groupsBefore = jooq.fetchCount(Tables.GROUP)
    val mappingsBefore = jooq.fetchCount(Tables.SCIM_RESOURCE_MAPPING)

    assertThatThrownBy {
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("Engineering", "rollback-group", listOf(member.id)))
        throw ExpectedFailure()
      }
    }.isInstanceOf(ExpectedFailure::class.java)

    assertThat(jooq.fetchCount(Tables.GROUP)).isEqualTo(groupsBefore)
    assertThat(jooq.fetchCount(Tables.SCIM_RESOURCE_MAPPING)).isEqualTo(mappingsBefore)
    assertThat(jooq.fetchCount(Tables.GROUP_MEMBER)).isZero()
  }

  @Test
  fun `normal mutation guards follow enabled and disabled matrices while permissions remain editable`() {
    val tenant = tenant("guard-matrix")
    val user = userMapping(tenant, "member@example.com", active = true)
    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("Engineering", null, emptyList()))
      }
    val airbyteGroup = requireNotNull(groupService.getGroup(GroupId(created.groupId)))

    assertThatThrownBy { groupService.updateGroup(airbyteGroup.copy(name = "Platform")) }
      .isInstanceOf(GroupManagedByScimException::class.java)
    assertThatThrownBy {
      groupService.addGroupMember(GroupId(created.groupId), UserId(user.userId), OrganizationId(tenant.organizationId))
    }.isInstanceOf(GroupManagedByScimException::class.java)
    assertThatThrownBy {
      groupService.removeGroupMember(GroupId(created.groupId), UserId(user.userId), OrganizationId(tenant.organizationId))
    }.isInstanceOf(GroupManagedByScimException::class.java)
    assertThatThrownBy { groupService.deleteGroup(GroupId(created.groupId), OrganizationId(tenant.organizationId)) }
      .isInstanceOf(GroupManagedByScimException::class.java)

    permissionRepository.save(
      Permission(
        groupId = created.groupId,
        organizationId = tenant.organizationId,
        permissionType = PermissionType.organization_reader,
      ),
    )
    assertThat(permissionRepository.findByGroupId(created.groupId)).hasSize(1)

    val disabledAt = OffsetDateTime.now()
    assertThat(
      configurationRepository.disableByIdAndOrganizationId(
        tenant.configurationId,
        tenant.organizationId,
        disabledAt,
        user.userId,
        disabledAt,
      ),
    ).isEqualTo(1)

    assertThat(groupService.updateGroup(airbyteGroup.copy(name = "Platform")).name).isEqualTo("Platform")
    groupService.addGroupMember(GroupId(created.groupId), UserId(user.userId), OrganizationId(tenant.organizationId))
    assertThat(groupService.isGroupMember(GroupId(created.groupId), UserId(user.userId))).isTrue()
    groupService.removeGroupMember(GroupId(created.groupId), UserId(user.userId), OrganizationId(tenant.organizationId))
    assertThat(groupService.isGroupMember(GroupId(created.groupId), UserId(user.userId))).isFalse()
    assertThatThrownBy { groupService.deleteGroup(GroupId(created.groupId), OrganizationId(tenant.organizationId)) }
      .isInstanceOf(GroupManagedByScimException::class.java)
  }

  @Test
  fun `disabled normal Group mutations advance SCIM lastModified only after actual changes`() {
    val tenant = tenant("disabled-last-modified")
    val user = userMapping(tenant, "member@example.com", active = true)
    val created =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("Engineering", null, emptyList()))
      }
    val airbyteGroup = requireNotNull(groupService.getGroup(GroupId(created.groupId)))
    val disabledAt = OffsetDateTime.now()
    assertThat(
      configurationRepository.disableByIdAndOrganizationId(
        tenant.configurationId,
        tenant.organizationId,
        disabledAt,
        user.userId,
        disabledAt,
      ),
    ).isEqualTo(1)
    val oldTimestamp = created.createdAt.minusDays(1)
    val resourceService = ScimGroupResourceService(objectMapper)

    fun ageMapping() {
      assertThat(
        jooq
          .update(Tables.SCIM_RESOURCE_MAPPING)
          .set(Tables.SCIM_RESOURCE_MAPPING.UPDATED_AT, oldTimestamp)
          .where(Tables.SCIM_RESOURCE_MAPPING.ID.eq(created.id))
          .and(Tables.SCIM_RESOURCE_MAPPING.SCIM_CONFIGURATION_ID.eq(tenant.configurationId))
          .and(Tables.SCIM_RESOURCE_MAPPING.ORGANIZATION_ID.eq(tenant.organizationId))
          .execute(),
      ).isEqualTo(1)
    }

    fun assertLastModifiedAdvanced() {
      val stored =
        requireNotNull(mappingRepository.findGroup(created.id, tenant.configurationId, tenant.organizationId))
      assertThat(stored.updatedAt).isAfter(oldTimestamp)
      val rendered =
        resourceService.completeResource(
          lifecycleService.get(tenant.configurationId, tenant.organizationId, created.id),
          URI.create("https://airbyte.example.com/"),
        )
      assertThat(OffsetDateTime.parse(rendered.path("meta").path("lastModified").asText())).isEqualTo(stored.updatedAt)
    }

    ageMapping()
    groupService.updateGroup(airbyteGroup.copy(name = "Platform"))
    assertLastModifiedAdvanced()

    ageMapping()
    groupService.addGroupMember(GroupId(created.groupId), UserId(user.userId), OrganizationId(tenant.organizationId))
    assertLastModifiedAdvanced()

    ageMapping()
    groupService.removeGroupMember(GroupId(created.groupId), UserId(user.userId), OrganizationId(tenant.organizationId))
    assertLastModifiedAdvanced()

    ageMapping()
    groupService.removeGroupMember(GroupId(created.groupId), UserId(user.userId), OrganizationId(tenant.organizationId))
    assertThat(requireNotNull(mappingRepository.findGroup(created.id, tenant.configurationId, tenant.organizationId)).updatedAt)
      .isEqualTo(oldTimestamp)
  }

  @Test
  fun `concurrent SCIM and normal case-variant creates serialize to one Group`() {
    val tenant = tenant("serialized-create")
    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val manual =
        executor.submit<Result<Any?>> {
          start.await()
          runCatching<Any?> { groupService.createGroup(airbyteGroup(tenant.organizationId, "Engineering")) }
        }
      val scim =
        executor.submit<Result<Any?>> {
          start.await()
          runCatching<Any?> {
            mutationService.execute(tenant.context) {
              lifecycleService.create(tenant.configurationId, tenant.organizationId, input("ENGINEERING", null, emptyList()))
            }
          }
        }

      start.countDown()
      val results = listOf(manual.get(), scim.get())

      assertThat(results.count { it.isSuccess }).isEqualTo(1)
      assertThat(results.single { it.isFailure }.exceptionOrNull())
        .isInstanceOfAny(GroupNameNotUniqueException::class.java, ScimGroupConflictException::class.java)
      assertThat(
        groupRepository
          .findAll()
          .filter { it.organizationId == tenant.organizationId && it.name.equals("engineering", ignoreCase = true) },
      ).hasSize(1)
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  fun `concurrent SCIM and normal case-variant renames serialize to one Group`() {
    val tenant = tenant("serialized-rename")
    val scimGroup =
      mutationService.execute(tenant.context) {
        lifecycleService.create(tenant.configurationId, tenant.organizationId, input("SCIM Source", null, emptyList()))
      }
    val manualGroup = groupService.createGroup(airbyteGroup(tenant.organizationId, "Manual Source"))
    val ready = CountDownLatch(2)
    val start = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val manual =
        executor.submit<Result<Any?>> {
          ready.countDown()
          start.await()
          runCatching<Any?> { groupService.updateGroup(manualGroup.copy(name = "Serialized Rename")) }
        }
      val scim =
        executor.submit<Result<Any?>> {
          ready.countDown()
          start.await()
          runCatching<Any?> {
            mutationService.execute(tenant.context) {
              lifecycleService.replace(
                tenant.configurationId,
                tenant.organizationId,
                scimGroup.id,
                input("SERIALIZED RENAME", null, emptyList()),
              )
            }
          }
        }

      jooq.transaction { configuration ->
        DSL
          .using(configuration)
          .selectFrom(Tables.ORGANIZATION)
          .where(Tables.ORGANIZATION.ID.eq(tenant.organizationId))
          .forUpdate()
          .fetchOne()
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue()
        start.countDown()
        assertThatThrownBy { manual.get(1, TimeUnit.SECONDS) }.isInstanceOf(TimeoutException::class.java)
        assertThatThrownBy { scim.get(1, TimeUnit.SECONDS) }.isInstanceOf(TimeoutException::class.java)
      }

      val manualResult = manual.get(10, TimeUnit.SECONDS)
      val scimResult = scim.get(10, TimeUnit.SECONDS)
      assertThat(listOf(manualResult, scimResult).count { it.isSuccess }).isEqualTo(1)
      if (manualResult.isFailure) {
        assertThat(manualResult.exceptionOrNull()).isInstanceOf(GroupNameNotUniqueException::class.java)
        assertThat(groupRepository.findById(manualGroup.groupId.value).orElseThrow().name).isEqualTo("Manual Source")
        assertThat(lifecycleService.get(tenant.configurationId, tenant.organizationId, scimGroup.id).displayName)
          .isEqualTo("SERIALIZED RENAME")
      } else {
        assertThat(scimResult.exceptionOrNull()).isInstanceOf(ScimGroupConflictException::class.java)
        assertThat(groupRepository.findById(manualGroup.groupId.value).orElseThrow().name).isEqualTo("Serialized Rename")
        assertThat(lifecycleService.get(tenant.configurationId, tenant.organizationId, scimGroup.id).displayName)
          .isEqualTo("SCIM Source")
      }
      assertThat(
        groupRepository
          .findAll()
          .filter { it.organizationId == tenant.organizationId && it.name.equals("serialized rename", ignoreCase = true) },
      ).hasSize(1)
    } finally {
      executor.shutdownNow()
    }
  }

  private fun tenant(name: String): Tenant {
    val resolvedOrganizationId =
      requireNotNull(
        organizationRepository
          .save(
            io.airbyte.data.repositories.entities
              .Organization(name = name, email = "$name@example.com"),
          ).id,
      )
    val tokenHash =
      UUID
        .randomUUID()
        .toString()
        .replace("-", "")
        .repeat(2)
    val configuration =
      configurationRepository.save(
        ScimConfiguration(
          organizationId = resolvedOrganizationId,
          tokenHash = tokenHash,
          idpProvider = "okta",
          enabled = true,
          tokenIssuedAt = OffsetDateTime.now(),
        ),
      )
    return Tenant(
      organizationId = resolvedOrganizationId,
      configurationId = requireNotNull(configuration.id),
      context = ScimAuthenticationContext(requireNotNull(configuration.id), OrganizationId(resolvedOrganizationId), tokenHash),
    )
  }

  private fun userMapping(
    tenant: Tenant,
    email: String,
    active: Boolean,
    displayName: String? = if (email.startsWith("alice")) "Alice Example" else email,
  ): UserMapping {
    val user = userRepository.save(ScimAirbyteUser(name = email.substringBefore('@'), email = email))
    val mapping =
      mappingRepository.save(
        ScimResourceMapping(
          scimConfigurationId = tenant.configurationId,
          organizationId = tenant.organizationId,
          resourceType = ScimResourceType.USER,
          userId = user.id,
          externalId = "external-$email",
          userName = email,
          primaryEmail = email,
          userActive = active,
          attributes =
            objectMapper.createObjectNode().also { attributes ->
              displayName?.let { attributes.put("displayName", it) }
            },
        ),
      )
    if (active) {
      permissionRepository.save(
        Permission(
          userId = user.id,
          organizationId = tenant.organizationId,
          permissionType = PermissionType.organization_member,
        ),
      )
    }
    return UserMapping(requireNotNull(mapping.id), user.id)
  }

  private fun input(
    displayName: String,
    externalId: String?,
    memberIds: List<UUID>,
  ): ScimGroupWrite = ScimGroupWrite(displayName, externalId, memberIds)

  private fun airbyteGroup(
    organizationId: UUID,
    name: String,
  ): io.airbyte.config.Group {
    val now = OffsetDateTime.now()
    return io.airbyte.config.Group(
      groupId = GroupId(UUID.randomUUID()),
      name = name,
      description = null,
      organizationId = OrganizationId(organizationId),
      memberCount = 0,
      createdAt = now,
      updatedAt = now,
    )
  }

  private data class Tenant(
    val organizationId: UUID,
    val configurationId: UUID,
    val context: ScimAuthenticationContext,
  )

  private data class UserMapping(
    val id: UUID,
    val userId: UUID,
  )

  private class ExpectedFailure : RuntimeException()

  companion object {
    private lateinit var context: ApplicationContext
    private lateinit var jooq: DSLContext
    private lateinit var objectMapper: ObjectMapper
    private lateinit var organizationRepository: OrganizationRepository
    private lateinit var configurationRepository: ScimConfigurationRepository
    private lateinit var mappingRepository: ScimResourceMappingRepository
    private lateinit var groupRepository: GroupRepository
    private lateinit var groupMemberRepository: GroupMemberRepository
    private lateinit var permissionRepository: PermissionRepository
    private lateinit var userRepository: ScimAirbyteUserRepository
    private lateinit var groupService: GroupService
    private lateinit var lifecycleService: ScimGroupLifecycleService
    private lateinit var mutationService: ScimMutationService

    private val container: PostgreSQLContainer<*> =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker")

    @BeforeAll
    @JvmStatic
    fun setUpDatabase() {
      container.start()
      context =
        ApplicationContext.run(
          PropertySource.of(
            "scim-group-lifecycle-test",
            mapOf(
              "datasources.config.driverClassName" to "org.postgresql.Driver",
              "datasources.config.db-type" to "postgres",
              "datasources.config.dialect" to "POSTGRES",
              "datasources.config.url" to container.jdbcUrl,
              "datasources.config.username" to container.username,
              "datasources.config.password" to container.password,
            ),
          ),
        )
      val dataSource =
        (context.getBean(DataSource::class.java, Qualifiers.byName("config")) as DelegatingDataSource)
          .targetDataSource
      jooq = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
      TestDatabaseProviders(dataSource, jooq).createNewConfigsDatabase()

      objectMapper = context.getBean(ObjectMapper::class.java)
      organizationRepository = context.getBean(OrganizationRepository::class.java)
      configurationRepository = context.getBean(ScimConfigurationRepository::class.java)
      mappingRepository = context.getBean(ScimResourceMappingRepository::class.java)
      groupRepository = context.getBean(GroupRepository::class.java)
      groupMemberRepository = context.getBean(GroupMemberRepository::class.java)
      permissionRepository = context.getBean(PermissionRepository::class.java)
      userRepository = context.getBean(ScimAirbyteUserRepository::class.java)
      groupService = context.getBean(GroupService::class.java)
      @Suppress("UNCHECKED_CAST")
      val transactions =
        context.getBean(TransactionOperations::class.java, Qualifiers.byName("config")) as TransactionOperations<Connection>
      lifecycleService = ScimGroupLifecycleService(mappingRepository, groupService)
      mutationService = ScimMutationService(organizationRepository, configurationRepository, transactions)
    }

    @AfterAll
    @JvmStatic
    fun tearDownDatabase() {
      context.close()
      container.close()
    }
  }
}
