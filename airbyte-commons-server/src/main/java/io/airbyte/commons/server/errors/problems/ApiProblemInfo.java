/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.problems;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airbyte.api.model.generated.KnownExceptionInfo;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;

@Introspected
public class ApiProblemInfo extends KnownExceptionInfo {

  @JsonProperty("type")
  private URI reference;
  private @Valid String title;

  public ApiProblemInfo title(final String title) {
    this.title = title;
    return this;
  }

  public ApiProblemInfo reference(final URI reference) {
    this.reference = reference;
    return this;
  }

  @Override
  public ApiProblemInfo exceptionClassName(final String name) {
    super.exceptionClassName(name);
    return this;
  }

  @Override
  public ApiProblemInfo message(final String message) {
    super.message(message);
    return this;
  }

  @Override
  public ApiProblemInfo exceptionStack(final List<String> stringList) {
    super.exceptionStack(stringList);
    return this;
  }

  public URI getReference() {
    return reference;
  }

  public void setReference(final URI reference) {
    this.reference = reference;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(final String title) {
    this.title = title;
  }

}
