/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

/**
 * How a saved authentication failure was tied to its data plane, strongest evidence first. A rejected exchange never proves
 * who sent it, so a failure is only replaced by one tied at least as strongly: a header anyone can send must not hide a
 * rejection of the data plane's own client ID.
 */
enum class DataplaneAuthenticationAssociation {
  /** The submitted client ID belongs to the data plane's service account. */
  REGISTERED_CLIENT_ID,

  /** The client ID matched no registered data plane, and the unauthenticated diagnostics header named this one. */
  CLAIMED_DIAGNOSTIC_ID,
  ;

  /** The saved associations that a failure tied this way may replace. */
  fun replaceableAssociations(): List<DataplaneAuthenticationAssociation> = entries.filter { it.ordinal >= ordinal }
}
