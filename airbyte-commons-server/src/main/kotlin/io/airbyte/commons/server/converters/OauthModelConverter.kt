/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.server.generated.models.AdvancedAuth
import io.airbyte.api.server.generated.models.OAuthConfigSpecification
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
    val isOAuth =
      List
        .of<io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType>(
          io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType.OAUTH_1_0,
          io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType.OAUTH_2_0,
        ).contains(incomingAdvancedAuth.authFlowType)
    if (!isOAuth) {
      return Optional.of(AdvancedAuth())
    }

    val oauthFlowType =
      if (io.airbyte.protocol.models.v0.AdvancedAuth.AuthFlowType.OAUTH_1_0 == incomingAdvancedAuth.authFlowType) {
        AdvancedAuth.AuthFlowType.OAUTH1_PERIOD0
      } else {
        AdvancedAuth.AuthFlowType.OAUTH2_PERIOD0
      }
    val incomingOAuthConfigSpecification =
      incomingAdvancedAuth.oauthConfigSpecification
    val advancedAuth =
      AdvancedAuth(
        authFlowType = oauthFlowType,
        predicateKey = incomingAdvancedAuth.predicateKey,
        predicateValue = incomingAdvancedAuth.predicateValue,
        oauthConfigSpecification =
          OAuthConfigSpecification(
            oauthUserInputFromConnectorConfigSpecification = incomingOAuthConfigSpecification.oauthUserInputFromConnectorConfigSpecification,
            completeOAuthOutputSpecification = incomingOAuthConfigSpecification.completeOauthOutputSpecification,
            completeOAuthServerInputSpecification = incomingOAuthConfigSpecification.completeOauthServerInputSpecification,
            completeOAuthServerOutputSpecification = incomingOAuthConfigSpecification.completeOauthServerOutputSpecification,
          ),
      )
    return Optional.of(advancedAuth)
  }
}
