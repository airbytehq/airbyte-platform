package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.UserInvitation
import io.airbyte.data.services.impls.data.mappers.EntityScopeType
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.InvitationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@MicronautTest
internal class UserInvitationRepositoryTest : AbstractConfigRepositoryTest<UserInvitationRepository>(UserInvitationRepository::class) {
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
      jooqDslContext.alterTable(
        Tables.USER_INVITATION,
      ).dropForeignKey(Keys.USER_INVITATION__USER_INVITATION_INVITER_USER_ID_FKEY.constraint()).execute()
    }
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
    val saveResult = repository.save(userInvitation)
    assert(repository.count() == 1L)

    val persistedInvitation = repository.findById(saveResult.id!!).get()

    assertInvitationEquals(userInvitation, persistedInvitation)
  }

  @Test
  fun `test find by invite code`() {
    val persistedInvitation = repository.save(userInvitation)
    assertEquals(repository.count(), 1L)

    val foundInvitation = repository.findByInviteCode(INVITE_CODE).get()
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
    repository.save(matchingWorkspaceInvite)

    val anotherMatchingInvite =
      matchingWorkspaceInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
      )
    repository.save(anotherMatchingInvite)

    val wrongStatusWorkspaceInvite =
      matchingWorkspaceInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        status = otherStatus,
      )
    repository.save(wrongStatusWorkspaceInvite)

    val wrongWorkspaceInvite =
      matchingWorkspaceInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherWorkspaceId,
      )
    repository.save(wrongWorkspaceInvite)

    val nothingMatchesWorkspaceInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherWorkspaceId,
        status = otherStatus,
      )
    repository.save(nothingMatchesWorkspaceInvite)

    // setup org invites

    val matchingOrgInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = organizationId,
        scopeType = ScopeType.organization,
        status = matchingStatus,
      )
    repository.save(matchingOrgInvite)

    val anotherMatchingOrgInvite =
      matchingOrgInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
      )
    repository.save(anotherMatchingOrgInvite)

    val wrongStatusOrgInvite =
      matchingOrgInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        status = otherStatus,
      )
    repository.save(wrongStatusOrgInvite)

    val wrongOrgInvite =
      matchingOrgInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherOrganizationId,
        status = matchingStatus,
      )
    repository.save(wrongOrgInvite)

    val nothingMatchesOrgInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherOrganizationId,
        scopeType = ScopeType.organization,
        status = otherStatus,
      )
    repository.save(nothingMatchesOrgInvite)

    val expectedWorkspaceMatches = listOf(matchingWorkspaceInvite, anotherMatchingInvite)
    val expectedOrgMatches = listOf(matchingOrgInvite, anotherMatchingOrgInvite)

    val actualWorkspaceInvites = repository.findByStatusAndScopeTypeAndScopeId(matchingStatus, EntityScopeType.workspace, workspaceId)
    val actualOrgInvites = repository.findByStatusAndScopeTypeAndScopeId(matchingStatus, EntityScopeType.organization, organizationId)

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
    repository.save(matchingInvite)

    val anotherMatchingInvite =
      matchingInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
      )
    repository.save(anotherMatchingInvite)

    val wrongEmailInvite =
      matchingInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        invitedEmail = otherEmail,
      )
    repository.save(wrongEmailInvite)

    val wrongWorkspaceInvite =
      matchingInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherWorkspaceId,
      )
    repository.save(wrongWorkspaceInvite)

    val wrongStatusInvite =
      matchingInvite.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        status = otherStatus,
      )
    repository.save(wrongStatusInvite)

    val wrongEverythingInvite =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        invitedEmail = otherEmail,
        scopeId = otherWorkspaceId,
        status = otherStatus,
      )
    repository.save(wrongEverythingInvite)

    val expectedMatches = listOf(matchingInvite, anotherMatchingInvite)
    val actualMatches =
      repository.findByStatusAndScopeTypeAndScopeIdAndInvitedEmail(
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
