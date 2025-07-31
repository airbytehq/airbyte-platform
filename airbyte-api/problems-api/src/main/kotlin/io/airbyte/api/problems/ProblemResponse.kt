/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.problems

/**
 * Interface to describe a Problem that can be returned by the API. Implementations are generated
 * from the api-problems.yaml openapi spec file.
 */
interface ProblemResponse {
  fun getStatus(): Int?

  fun getTitle(): String?

  fun getType(): String?

  fun getDocumentationUrl(): String?

  fun getDetail(): String?

  fun getData(): Any?
}
