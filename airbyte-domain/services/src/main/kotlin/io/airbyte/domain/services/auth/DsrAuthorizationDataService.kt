/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.repositories.DataSubjectDeletionRequestRepository
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class DsrAuthorizationDataService(
  private val requests: DataSubjectDeletionRequestRepository,
) {
  fun exists(requestId: UUID): Boolean = requests.existsById(requestId)
}
