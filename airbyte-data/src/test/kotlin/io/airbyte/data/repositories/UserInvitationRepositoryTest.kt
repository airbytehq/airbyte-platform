/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.UserInvitation
import io.airbyte.data.services.impls.data.mappers.EntityScopeType
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.InvitationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@MicronautTest
internal class UserInvitationRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    const val INVITE_CODE = "some-code"
    val EXPIRES_AT = OffsetDateTime.now(ZoneOffset.UTC).plusDays(7).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)

    val userInvitation =
      UserInvitation(
        inviteCode = INVITE_CODE,
        inviterUserId = UUID.randomUUID(),
        invitedEmail = "invited@airbyte.io",
        scopeId = UUID.randomUUID(),
        scopeType = ScopeType.workspace,
        permissionType = PermissionType.workspace_admin,
        status = InvitationStatus.pending,
        expiresAt = EXPIRES_AT,
      )

    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making users as well
      jooqDslContext
        .alterTable(
          Tables.USER_INVITATION,
        ).dropForeignKey(Keys.USER_INVITATION__USER_INVITATION_INVITER_USER_ID_FKEY.constraint())
        .execute()
    }
  }

  @AfterEach
  fun tearDown() {
    userInvitationRepository.deleteAll()
  }

  private fun assertInvitationEquals(
    expected: UserInvitation,
    actual: UserInvitation,
  ) {
    with(actual) {
      assertEquals(id, expected.id)
      assertEquals(inviteCode, expected.inviteCode)
      assertEquals(inviterUserId, expected.inviterUserId)
      assertEquals(invitedEmail, expected.invitedEmail)
      assertEquals(scopeId, expected.scopeId)
      assertEquals(scopeType, expected.scopeType)
      assertEquals(permissionType, expected.permissionType)
      assertEquals(status, expected.status)

      // convert to granularity of seconds to avoid failure due to differences in precision
      assertEquals(createdAt?.toEpochSecond(), expected.createdAt?.toEpochSecond())
      assertEquals(updatedAt?.toEpochSecond(), expected.updatedAt?.toEpochSecond())
    }
  }

  @Test
  fun `test db insertion`() {
    val saveResult = userInvitationRepository.save(userInvitation)
    assert(userInvitationRepository.count() == 1L)

    val persistedInvitation = userInvitationRepository.findById(saveResult.id!!).get()

    assertInvitationEquals(userInvitation, persistedInvitation)
  }

  @Test
  fun `test find by invite code`() {
    val persistedInvitation = userInvitationRepository.save(userInvitation)
    assertEquals(userInvitationRepository.count(), 1L)

    val foundInvitation = userInvitationRepository.findByInviteCode(INVITE_CODE).get()
    assertInvitationEquals(persistedInvitation, foundInvitation)
  }

  @Test
  fun `test find by status and scope type and scope id`() {
    val workspaceId = UUID.randomUUID()
    val otherWorkspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val matchingStatus = InvitationStatus.pending
    val otherStatus = InvitationStatus.accepted

    // setup workspace invites

    val matchingWorkspaceInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = workspaceId,
        status = matchingStatus,
      )
    userInvitationRepository.save(matchingWorkspaceInvite)

    val anotherMatchingInvite =
      matchingWorkspaceInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
      )
    userInvitationRepository.save(anotherMatchingInvite)

    val wrongStatusWorkspaceInvite =
      matchingWorkspaceInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        status = otherStatus,
      )
    userInvitationRepository.save(wrongStatusWorkspaceInvite)

    val wrongWorkspaceInvite =
      matchingWorkspaceInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherWorkspaceId,
      )
    userInvitationRepository.save(wrongWorkspaceInvite)

    val nothingMatchesWorkspaceInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherWorkspaceId,
        status = otherStatus,
      )
    userInvitationRepository.save(nothingMatchesWorkspaceInvite)

    // setup org invites

    val matchingOrgInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        permissionType = PermissionType.organization_admin,
        scopeId = organizationId,
        scopeType = ScopeType.organization,
        status = matchingStatus,
      )
    userInvitationRepository.save(matchingOrgInvite)

    val anotherMatchingOrgInvite =
      matchingOrgInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
      )
    userInvitationRepository.save(anotherMatchingOrgInvite)

    val wrongStatusOrgInvite =
      matchingOrgInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        status = otherStatus,
      )
    userInvitationRepository.save(wrongStatusOrgInvite)

    val wrongOrgInvite =
      matchingOrgInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherOrganizationId,
        status = matchingStatus,
      )
    userInvitationRepository.save(wrongOrgInvite)

    val nothingMatchesOrgInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        permissionType = PermissionType.organization_admin,
        scopeId = otherOrganizationId,
        scopeType = ScopeType.organization,
        status = otherStatus,
      )
    userInvitationRepository.save(nothingMatchesOrgInvite)

    val expectedWorkspaceMatches = listOf(matchingWorkspaceInvite, anotherMatchingInvite)
    val expectedOrgMatches = listOf(matchingOrgInvite, anotherMatchingOrgInvite)

    val actualWorkspaceInvites = userInvitationRepository.findByStatusAndScopeTypeAndScopeId(matchingStatus, EntityScopeType.workspace, workspaceId)
    val actualOrgInvites = userInvitationRepository.findByStatusAndScopeTypeAndScopeId(matchingStatus, EntityScopeType.organization, organizationId)

    // for each workspace invitation found, make sure that it has a match by calling assertInvitationEquals
    expectedWorkspaceMatches.forEach { expected ->
      val actual = actualWorkspaceInvites.find { it.id == expected.id }
      assert(actual != null)
      assertInvitationEquals(expected, actual!!)
    }

    // for each organization invitation found, make sure that it has a match by calling assertInvitationEquals
    expectedOrgMatches.forEach { expected ->
      val actual = actualOrgInvites.find { it.id == expected.id }
      assert(actual != null)
      assertInvitationEquals(expected, actual!!)
    }
  }

  @Test
  fun `test find by status and scope type and scope id and invited email`() {
    val workspaceId = UUID.randomUUID()
    val otherWorkspaceId = UUID.randomUUID()
    val matchingStatus = InvitationStatus.pending
    val otherStatus = InvitationStatus.accepted
    val matchingEmail = "matching@airbyte.io"
    val otherEmail = "other@airbyte.io"

    val matchingInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = workspaceId,
        status = matchingStatus,
        invitedEmail = matchingEmail,
      )
    userInvitationRepository.save(matchingInvite)

    val anotherMatchingInvite =
      matchingInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
      )
    userInvitationRepository.save(anotherMatchingInvite)

    val wrongEmailInvite =
      matchingInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        invitedEmail = otherEmail,
      )
    userInvitationRepository.save(wrongEmailInvite)

    val wrongWorkspaceInvite =
      matchingInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherWorkspaceId,
      )
    userInvitationRepository.save(wrongWorkspaceInvite)

    val wrongStatusInvite =
      matchingInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        status = otherStatus,
      )
    userInvitationRepository.save(wrongStatusInvite)

    val wrongEverythingInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        invitedEmail = otherEmail,
        scopeId = otherWorkspaceId,
        status = otherStatus,
      )
    userInvitationRepository.save(wrongEverythingInvite)

    val expectedMatches = listOf(matchingInvite, anotherMatchingInvite)
    val actualMatches =
      userInvitationRepository.findByStatusAndScopeTypeAndScopeIdAndInvitedEmail(
        matchingStatus,
        EntityScopeType.workspace,
        workspaceId,
        matchingEmail,
      )

    // for each invitation found, make sure that it has a match by calling assertInvitationEquals
    expectedMatches.forEach { expected ->
      val actual = actualMatches.find { it.id == expected.id }
      assert(actual != null)
      assertInvitationEquals(expected, actual!!)
    }
  }
}
