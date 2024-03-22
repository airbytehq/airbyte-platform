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

    val matchingWorkspaceAndStatusInvitation =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = workspaceId,
        status = matchingStatus,
      )
    repository.save(matchingWorkspaceAndStatusInvitation)

    val anotherMatchingWorkspaceAndStatusInvitation =
      matchingWorkspaceAndStatusInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
      )
    repository.save(anotherMatchingWorkspaceAndStatusInvitation)

    val matchingWorkspaceWrongStatusInvitation =
      matchingWorkspaceAndStatusInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        status = otherStatus,
      )
    repository.save(matchingWorkspaceWrongStatusInvitation)

    val wrongWorkspaceMatchingStatusInvitation =
      matchingWorkspaceAndStatusInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherWorkspaceId,
      )
    repository.save(wrongWorkspaceMatchingStatusInvitation)

    val matchingOrganizationAndStatusInvitation =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = organizationId,
        scopeType = ScopeType.organization,
        status = matchingStatus,
      )
    repository.save(matchingOrganizationAndStatusInvitation)

    val anotherMatchingOrganizationAndStatusInvitation =
      matchingOrganizationAndStatusInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
      )
    repository.save(anotherMatchingOrganizationAndStatusInvitation)

    val matchingOrganizationWrongStatusInvitation =
      matchingOrganizationAndStatusInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        status = otherStatus,
      )
    repository.save(matchingOrganizationWrongStatusInvitation)

    val wrongOrganizationMatchingStatusInvitation =
      matchingOrganizationAndStatusInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherOrganizationId,
        status = matchingStatus,
      )
    repository.save(wrongOrganizationMatchingStatusInvitation)

    val nothingMatchesInvitation =
      userInvitation.copy(
        id = UUID.randomUUID(),
        inviteCode = UUID.randomUUID().toString(),
        scopeId = otherWorkspaceId,
        status = otherStatus,
      )
    repository.save(nothingMatchesInvitation)

    val expectedWorkspaceMatches = listOf(matchingWorkspaceAndStatusInvitation, anotherMatchingWorkspaceAndStatusInvitation)
    val expectedOrganizationMatches = listOf(matchingOrganizationAndStatusInvitation, anotherMatchingOrganizationAndStatusInvitation)

    val actualWorkspaceInvitations = repository.findByStatusAndScopeTypeAndScopeId(matchingStatus, EntityScopeType.workspace, workspaceId)
    val actualOrganizationInvitations = repository.findByStatusAndScopeTypeAndScopeId(matchingStatus, EntityScopeType.organization, organizationId)

    // for each worksapce invitation found, make sure that it has a match by calling assertInvitationEquals
    expectedWorkspaceMatches.forEach { expected ->
      val actual = actualWorkspaceInvitations.find { it.id == expected.id }
      assert(actual != null)
      assertInvitationEquals(expected, actual!!)
    }

    // for each organization invitation found, make sure that it has a match by calling assertInvitationEquals
    expectedOrganizationMatches.forEach { expected ->
      val actual = actualOrganizationInvitations.find { it.id == expected.id }
      assert(actual != null)
      assertInvitationEquals(expected, actual!!)
    }
  }
}
