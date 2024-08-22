/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.problems

abstract class AbstractThrowableProblem(val problem: ProblemResponse) : RuntimeException(
  problem.detail,
)
