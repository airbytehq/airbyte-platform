/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.roles

interface AuthRoleInterface {
  fun getAuthority(): Int

  fun getLabel(): String
}
