/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Notification Service.
 */
public interface NotificationService {

  // TODO: the List<NotificationConfigurationRecord> is what was here originally, but that is jooq
  // specific.
  List<Object> getNotificationConfigurationByConnectionIds(List<UUID> connnectionIds) throws IOException;

}
