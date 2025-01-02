/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.problems

abstract class AbstractThrowableProblem(val problem: ProblemResponse) : RuntimeException(
  problem.detail,
)
