/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.problems;

import io.micronaut.http.HttpStatus;
import jakarta.validation.Valid;
import java.io.Serial;
import java.net.URI;
import lombok.Getter;

@Getter
public abstract class AbstractThrowableProblem extends RuntimeException {

  @Serial
  private static final long serialVersionUID = 4455758464029365605L;
  private URI reference;
  private @Valid String title;
  private @Valid HttpStatus httpStatus;

  public AbstractThrowableProblem(final String message) {
    super(message);
  }

  public AbstractThrowableProblem(final URI reference, final String title, final HttpStatus httpStatus, final String message) {
    super(message);
    this.reference = reference;
    this.httpStatus = httpStatus;
    this.title = title;
  }

  public AbstractThrowableProblem title(final String title) {
    this.title = title;
    return this;
  }

  public AbstractThrowableProblem reference(final URI type) {
    this.reference = type;
    return this;
  }

  public void setReference(final URI reference) {
    this.reference = reference;
  }

  public void setTitle(final String title) {
    this.title = title;
  }

  public int getHttpCode() {
    return httpStatus.getCode();
  }

  public ApiProblemInfo getApiProblemInfo() {
    return infoFromThrowable(this);
  }

  /**
   * Static factory for creating a known exception.
   *
   * @param t throwable to wrap
   * @param message error message
   * @return known exception
   */
  public static ApiProblemInfo infoFromThrowableWithMessage(final AbstractThrowableProblem t, final String message) {
    return new ApiProblemInfo()
        .message(message)
        .reference(t.getReference())
        .title(t.getTitle());
  }

  public static ApiProblemInfo infoFromThrowable(final AbstractThrowableProblem t) {
    return infoFromThrowableWithMessage(t, t.getMessage());
  }

}
