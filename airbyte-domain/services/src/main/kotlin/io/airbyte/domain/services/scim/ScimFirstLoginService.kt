/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.config.AuthProvider
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimAuthUserRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.ScimAuthUser
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.Locale
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider as DbAuthProvider

@Singleton
open class ScimFirstLoginService(
  private val mappingRepository: ScimResourceMappingRepository,
  private val userRepository: ScimAirbyteUserRepository,
  private val authUserRepository: ScimAuthUserRepository,
) {
  @Transactional("config")
  open fun attachIfPreProvisioned(
    email: String,
    verifiedEmail: String?,
    authUserId: String,
    authProvider: AuthProvider?,
    accessOrganizationId: UUID? = null,
    expectedUnmappedUserId: UUID? = null,
  ): ScimFirstLoginAttachmentResult {
    // Permission grants later in an SSO login lock this configuration and reference its
    // organization. Match SCIM mutation's global organization -> configuration -> email order.
    accessOrganizationId?.let {
      if (mappingRepository.findOrganizationIdByIdForUpdate(it) == null) {
        return ScimFirstLoginAttachmentResult.Conflict
      }
      mappingRepository.findConfigurationIdByOrganizationIdForUpdate(it)
    }
    val claimedEmails =
      listOfNotNull(email, verifiedEmail)
        .associateBy { it.lowercase(Locale.ROOT) }
        .toSortedMap()
    claimedEmails.values.forEach(userRepository::acquireGlobalEmailLock)
    val mappedUserIdsByEmail =
      claimedEmails.mapValues { (_, claimedEmail) ->
        mappingRepository
          .findUsersByPrimaryEmailForUpdate(claimedEmail)
          .map { it.userId }
          .distinct()
      }
    val verifiedEmailKey = verifiedEmail?.lowercase(Locale.ROOT)
    val mappedUserIds = verifiedEmailKey?.let(mappedUserIdsByEmail::get).orEmpty()
    val unverifiedMappedUserIds =
      mappedUserIdsByEmail
        .filterKeys { it != verifiedEmailKey }
        .values
        .flatten()
        .distinct()
    val expectedUserHasMapping =
      expectedUnmappedUserId
        ?.let(mappingRepository::findUsersByUserIdForUpdate)
        ?.isNotEmpty() == true
    authUserRepository.acquireIdentityLock(authUserId)
    val existingIdentities = authUserRepository.findByAuthUserIdForUpdate(authUserId)
    val existingIdentityOwners = existingIdentities.map { it.userId }.distinct()

    if (mappedUserIds.isNotEmpty() && unverifiedMappedUserIds.any { it !in mappedUserIds }) {
      return ScimFirstLoginAttachmentResult.Conflict
    }
    if (mappedUserIds.isEmpty()) {
      if (unverifiedMappedUserIds.isNotEmpty()) {
        if (unverifiedMappedUserIds.size != 1) {
          return ScimFirstLoginAttachmentResult.Conflict
        }
        val unverifiedMappedUserId = unverifiedMappedUserIds.single()
        if (existingIdentityOwners.any { it != unverifiedMappedUserId }) {
          return ScimFirstLoginAttachmentResult.Conflict
        }
        if (existingIdentities.isNotEmpty()) {
          return ScimFirstLoginAttachmentResult.AlreadyAttached(unverifiedMappedUserId)
        }
        return ScimFirstLoginAttachmentResult.EmailNotVerified
      }
      return when (existingIdentityOwners.size) {
        0 ->
          if (expectedUserHasMapping) {
            ScimFirstLoginAttachmentResult.Conflict
          } else {
            ScimFirstLoginAttachmentResult.NoMatch
          }
        1 -> ScimFirstLoginAttachmentResult.ExistingIdentity(existingIdentityOwners.single())
        else -> ScimFirstLoginAttachmentResult.AmbiguousIdentity
      }
    }
    if (mappedUserIds.size != 1) {
      return ScimFirstLoginAttachmentResult.Conflict
    }

    val mappedUserId = mappedUserIds.single()
    if (existingIdentityOwners.any { it != mappedUserId }) {
      return ScimFirstLoginAttachmentResult.Conflict
    }
    if (existingIdentities.isNotEmpty()) {
      return ScimFirstLoginAttachmentResult.AlreadyAttached(mappedUserId)
    }
    val dbAuthProvider =
      authProvider?.value()?.let(DbAuthProvider::lookupLiteral)
        ?: return ScimFirstLoginAttachmentResult.Conflict

    authUserRepository.save(
      ScimAuthUser(
        userId = mappedUserId,
        authUserId = authUserId,
        authProvider = dbAuthProvider,
      ),
    )
    return ScimFirstLoginAttachmentResult.Attached(mappedUserId)
  }

  fun isScimManagedUser(userId: UUID): Boolean = mappingRepository.existsUserMappingByUserId(userId)
}

sealed interface ScimFirstLoginAttachmentResult {
  data object NoMatch : ScimFirstLoginAttachmentResult

  data class ExistingIdentity(
    val userId: UUID,
  ) : ScimFirstLoginAttachmentResult

  data object AmbiguousIdentity : ScimFirstLoginAttachmentResult

  data object EmailNotVerified : ScimFirstLoginAttachmentResult

  data object Conflict : ScimFirstLoginAttachmentResult

  data class Attached(
    val userId: UUID,
  ) : ScimFirstLoginAttachmentResult

  data class AlreadyAttached(
    val userId: UUID,
  ) : ScimFirstLoginAttachmentResult
}
