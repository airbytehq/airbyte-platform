/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import io.airbyte.api.model.generated.AdvancedAuth
import io.airbyte.api.model.generated.OAuthConfigSpecification

/**
 * Utility class to handle advanced authentication mapping and configuration.
 */
class ConfigTemplateAdvancedAuthHelper {
  companion object {
    /**
     * Maps v0 AdvancedAuth model to the generated API AdvancedAuth model.
     *
     * @param sourceAdvancedAuth The source v0 advanced auth configuration
     * @return A new AdvancedAuth object populated with the mapped values
     */
    fun mapAdvancedAuth(sourceAdvancedAuth: io.airbyte.protocol.models.v0.AdvancedAuth): AdvancedAuth =
      AdvancedAuth()
        .authFlowType(
          when (sourceAdvancedAuth.authFlowType) {
            io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType.OAUTH_1_0 -> AdvancedAuth.AuthFlowTypeEnum.OAUTH1_0
            io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType.OAUTH_2_0 -> AdvancedAuth.AuthFlowTypeEnum.OAUTH2_0
            null -> AdvancedAuth.AuthFlowTypeEnum.OAUTH2_0
          },
        ).predicateKey(sourceAdvancedAuth.predicateKey)
        .predicateValue(sourceAdvancedAuth.predicateValue)
        .oauthConfigSpecification(
          OAuthConfigSpecification()
            .completeOAuthOutputSpecification(
              sourceAdvancedAuth.oauthConfigSpecification.completeOauthOutputSpecification,
            ).oauthUserInputFromConnectorConfigSpecification(
              sourceAdvancedAuth.oauthConfigSpecification.oauthUserInputFromConnectorConfigSpecification,
            ).completeOAuthServerInputSpecification(
              sourceAdvancedAuth.oauthConfigSpecification.completeOauthServerInputSpecification,
            ).completeOAuthServerOutputSpecification(
              sourceAdvancedAuth.oauthConfigSpecification.completeOauthServerOutputSpecification,
            ),
        )
  }
}
