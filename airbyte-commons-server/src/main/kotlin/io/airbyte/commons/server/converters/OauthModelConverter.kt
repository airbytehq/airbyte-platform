/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.AdvancedAuth
import io.airbyte.api.model.generated.AdvancedAuth.AuthFlowTypeEnum
import io.airbyte.api.model.generated.OAuthConfigSpecification
import io.airbyte.protocol.models.v0.ConnectorSpecification
import java.util.List
import java.util.Optional

/**
 * Extract OAuth models from connector specs.
 */
object OauthModelConverter {
  /**
   * Get advanced (new and preferred) auth from a connector spec.
   *
   * @param spec connector spec
   * @return advanced auth if present.
   */
  @JvmStatic
  fun getAdvancedAuth(spec: ConnectorSpecification): Optional<AdvancedAuth> {
    if (spec.advancedAuth == null) {
      return Optional.empty()
    }
    val incomingAdvancedAuth = spec.advancedAuth
    val advancedAuth = AdvancedAuth()
    if (List
        .of<io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType>(
          io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType.OAUTH_1_0,
          io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType.OAUTH_2_0,
        ).contains(incomingAdvancedAuth.authFlowType)
    ) {
      val oauthFlowType =
        if (io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType.OAUTH_1_0 == incomingAdvancedAuth.authFlowType) {
          AuthFlowTypeEnum.OAUTH1_0
        } else {
          AuthFlowTypeEnum.OAUTH2_0
        }
      val incomingOAuthConfigSpecification =
        incomingAdvancedAuth.oauthConfigSpecification
      advancedAuth
        .authFlowType(oauthFlowType)
        .predicateKey(incomingAdvancedAuth.predicateKey)
        .predicateValue(incomingAdvancedAuth.predicateValue)
        .oauthConfigSpecification(
          OAuthConfigSpecification()
            .oauthUserInputFromConnectorConfigSpecification(
              incomingOAuthConfigSpecification.oauthUserInputFromConnectorConfigSpecification,
            ).completeOAuthOutputSpecification(incomingOAuthConfigSpecification.completeOauthOutputSpecification)
            .completeOAuthServerInputSpecification(incomingOAuthConfigSpecification.completeOauthServerInputSpecification)
            .completeOAuthServerOutputSpecification(incomingOAuthConfigSpecification.completeOauthServerOutputSpecification),
        )
    }
    return Optional.of(advancedAuth)
  }
}
