/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.domain;

public enum InvitationStatus {
  PENDING,
  ACCEPTED,
  CANCELLED,
  DECLINED
}
