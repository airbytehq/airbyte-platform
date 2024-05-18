package io.airbyte.data.config

import io.micronaut.context.annotation.ConfigurationProperties

@ConfigurationProperties("airbyte.auth.instanceAdmin")
open class InstanceAdminConfig {
  var username: String? = null
  var password: String? = null
  var clientId: String? = null
  var clientSecret: String? = null
}
