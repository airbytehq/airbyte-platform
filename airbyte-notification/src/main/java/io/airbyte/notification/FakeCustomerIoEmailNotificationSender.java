/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fake client for customerIoNotification - this will not send any actual emails.
 */
@Singleton
public class FakeCustomerIoEmailNotificationSender extends CustomerIoEmailNotificationSender {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  FakeCustomerIoEmailNotificationSender() {
    super(null, null);
  }

  @Override
  protected void callCustomerIoSendNotification(final String custumerIoPayload) {
    log.info("FakeCustomerIoEmailNotificationSender: callCustomerIoSendNotification: {}", custumerIoPayload);
  }

}
