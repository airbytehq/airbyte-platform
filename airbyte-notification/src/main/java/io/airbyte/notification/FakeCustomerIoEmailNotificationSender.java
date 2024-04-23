/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Fake client for customerIoNotification - this will not send any actual emails.
 */
@Slf4j
@Singleton
public class FakeCustomerIoEmailNotificationSender extends CustomerIoEmailNotificationSender {

  FakeCustomerIoEmailNotificationSender() {
    super(null, null);
  }

  @Override
  protected void callCustomerIoSendNotification(final String custumerIoPayload) {
    log.info("FakeCustomerIoEmailNotificationSender: callCustomerIoSendNotification: {}", custumerIoPayload);
  }

}
