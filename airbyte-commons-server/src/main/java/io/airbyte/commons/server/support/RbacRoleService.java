/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.micronaut.http.HttpRequest;
import java.util.Collection;

public interface RbacRoleService {

  Collection<String> getRbacRoles(final String authUserId, final HttpRequest<?> request);

}
