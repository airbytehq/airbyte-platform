/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Fake client for customerIoNotification - this will not send any actual emails.
 */
@Slf4j
@Singleton
@Requires(env = "local-test")
public class FakeCustomerIoEmailNotificationSender extends CustomerIoEmailNotificationSender {

  FakeCustomerIoEmailNotificationSender() {
    super(null, null);
  }

  @Override
  protected void callCustomerIoSendNotification(final String custumerIoPayload) {
    log.info("FakeCustomerIoEmailNotificationSender: callCustomerIoSendNotification: {}", custumerIoPayload);
  }

}
