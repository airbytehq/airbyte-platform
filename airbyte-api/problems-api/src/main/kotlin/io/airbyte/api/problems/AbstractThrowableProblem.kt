/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.problems

abstract class AbstractThrowableProblem(
  val problem: ProblemResponse,
) : RuntimeException(
    "Detail: $problem.detail, Data: ${problem.getData()}",
  )
