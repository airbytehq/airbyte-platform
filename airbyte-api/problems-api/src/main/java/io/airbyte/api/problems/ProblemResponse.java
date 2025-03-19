/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.problems;

/**
 * Interface to describe a Problem that can be returned by the API. Implementations are generated
 * from the api-problems.yaml openapi spec file.
 */
public interface ProblemResponse {

  Integer getStatus();

  String getTitle();

  String getType();

  String getDocumentationUrl();

  String getDetail();

  Object getData();

}
