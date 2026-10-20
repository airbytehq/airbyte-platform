/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.route

import io.micronaut.security.authentication.Authentication

/** Rules copy parsed arguments into immutable values before resolving and checking saved ownership. */
interface OperationInput

interface OperationRule {
  fun readInput(args: Array<out Any?>): OperationInput

  fun authorize(
    caller: Authentication,
    input: OperationInput,
  )
}
