package io.airbyte.api.problems

abstract class AbstractThrowableProblem(val problem: ProblemResponse) : RuntimeException(
  problem.detail,
)
