package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.UserInvitation
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.InvitationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
internal class UserInvitationRepositoryTest : AbstractConfigRepositoryTest<UserInvitationRepository>(UserInvitationRepository::class) {
  companion object {
    const val INVITE_CODE = "some-code"

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
    val invitation =
      UserInvitation(
        inviteCode = INVITE_CODE,
        inviterUserId = UUID.randomUUID(),
        invitedEmail = "invited@airbyte.io",
        scopeId = UUID.randomUUID(),
        scopeType = ScopeType.workspace,
        permissionType = PermissionType.workspace_admin,
        status = InvitationStatus.pending,
      )
    val saveResult = repository.save(invitation)
    assert(repository.count() == 1L)

    val persistedInvitation = repository.findById(saveResult.id!!).get()

    assertInvitationEquals(invitation, persistedInvitation)
  }

  @Test
  fun `test find by invite code`() {
    val invitation =
      UserInvitation(
        inviteCode = INVITE_CODE,
        inviterUserId = UUID.randomUUID(),
        invitedEmail = "invited@airbyte.io",
        scopeId = UUID.randomUUID(),
        scopeType = ScopeType.workspace,
        permissionType = PermissionType.workspace_admin,
        status = InvitationStatus.pending,
      )
    val persistedInvitation = repository.save(invitation)
    assertEquals(repository.count(), 1L)

    val foundInvitation = repository.findByInviteCode(INVITE_CODE).get()
    assertInvitationEquals(persistedInvitation, foundInvitation)
  }
}
