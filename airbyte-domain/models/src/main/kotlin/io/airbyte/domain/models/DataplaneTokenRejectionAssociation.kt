/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.util.UUID

/**
 * The registered data plane a rejected token exchange is attributed to. The exchange failed, so no association proves who
 * sent the request.
 */
sealed interface DataplaneTokenRejectionAssociation {
  /**
   * The submitted client ID belongs to this data plane's service account and the secret did not match.
   */
  data class RegisteredClientId(
    val dataplaneId: UUID,
    val organizationId: UUID,
  ) : DataplaneTokenRejectionAssociation

  /**
   * The submitted client ID matched no registered data plane, and the unauthenticated diagnostics header named this one.
   */
  data class ClaimedDiagnosticId(
    val dataplaneId: UUID,
    val organizationId: UUID,
  ) : DataplaneTokenRejectionAssociation

  /**
   * Nothing in the request matched a registered data plane, so the rejection is only reported to operational telemetry.
   */
  data object Unassociated : DataplaneTokenRejectionAssociation
}
