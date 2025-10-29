/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Amazon Sellers OAuth.
 */
class AmazonSellerPartnerOAuthFlow : BaseOAuth2Flow {
  override fun getClientIdUnsafe(oauthConfig: JsonNode): String = getConfigValueUnsafe(oauthConfig, "lwa_app_id")

  override fun getClientSecretUnsafe(oauthConfig: JsonNode): String = getConfigValueUnsafe(oauthConfig, "lwa_client_secret")

  constructor(httpClient: HttpClient) : super(httpClient)

  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

    /*
     * Overriden default method to provide possibility to retrieve `app_id` from
     * `sourceOAuthParamConfig` bypassing `formatConsentUrl()` method.
     */
  override fun getSourceConsentUrl(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification?,
    sourceOAuthParamConfig: JsonNode?,
  ): String {
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration)

    if (sourceOAuthParamConfig == null) {
      throw ResourceNotFoundProblem(
        "Undefined OAuth Parameter.",
        ProblemResourceData().resourceType(ConfigNotFoundType.SOURCE_OAUTH_PARAM.name),
      )
    }

    try {
      val accountType = getConfigValueUnsafe(inputOAuthConfiguration, "account_type")
      val region = getConfigValueUnsafe(inputOAuthConfiguration, "region")

      val consentUrl =
        if ("Vendor" == accountType) {
          VENDOR_CENTRAL_URLS[region]
        } else {
          SELLER_CENTRAL_URLS[region]
        }

              /*
               * Airbyte Amazon Seller Partner `application_id`, to provide native OAuth integration for
               * 3rd-parties.
               */
      return URIBuilder("$consentUrl/apps/authorize/consent")
        // get the `app_id` parameter from instance-wide params
        .addParameter("application_id", getConfigValueUnsafe(sourceOAuthParamConfig, "app_id"))
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter("state", getState())
            /*
             * Use `version=beta` for OAuth tests only, or when the OAuth App is in `draft` status
             * https://developer-docs.amazon.com/amazon-shipping/docs/authorizing-selling-partner-api-
             * applications#constructing-an-oauth-authorization-uri .addParameter("version", "beta")
             */
        .build()
        .toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun extractCodeParameter(queryParams: Map<String, Any>): String {
    if (queryParams.containsKey("spapi_oauth_code")) {
      return queryParams["spapi_oauth_code"] as String
    } else {
      throw IOException("Undefined 'spapi_oauth_code' from consent redirected url.")
    }
  }

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    mapOf(
      CLIENT_ID_KEY to clientId,
      REDIRECT_URI_KEY to redirectUrl,
      CLIENT_SECRET_KEY to clientSecret,
      AUTH_CODE_KEY to authCode,
      GRANT_TYPE_KEY to "authorization_code",
    )

  /**
   * Returns the URL where to retrieve the access token from.
   *
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun getDefaultOAuthOutputPath(): List<String> = listOf()

    /*
     * Should be overriden to satisfy the BaseOAuth2Flow abstraction requirements
     */
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String = ""

  companion object {
    private const val ACCESS_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
    private const val SELLER_EUROPE_URL = "https://sellercentral-europe.amazon.com"
    private val VENDOR_CENTRAL_URLS: Map<String, String> =
      mapOf(
        "CA" to "https://vendorcentral.amazon.ca",
        "US" to "https://vendorcentral.amazon.com",
        "DE" to "https://vendorcentral.amazon.de",
        "MX" to "https://vendorcentral.amazon.com.mx",
        "JP" to "https://vendorcentral.amazon.co.jp",
        "IT" to "https://vendorcentral.amazon.it",
        "AU" to "https://vendorcentral.amazon.com.au",
        "BR" to "https://vendorcentral.amazon.com.br",
        "BE" to "https://vendorcentral.amazon.com.be",
        "ES" to "https://vendorcentral.amazon.es",
        "UK" to "https://vendorcentral.amazon.co.uk",
        "GB" to "https://vendorcentral.amazon.co.uk",
        "NL" to "https://vendorcentral.amazon.nl",
        "PL" to "https://vendorcentral.amazon.pl",
        "FR" to "https://vendorcentral.amazon.fr",
        "IN" to "https://www.vendorcentral.in",
        "SE" to "https://vendorcentral.amazon.se",
        "SG" to "https://vendorcentral.amazon.com.sg",
        "AE" to "https://vendorcentral.amazon.me",
        "TR" to "https://vendorcentral.amazon.com.tr",
        "SA" to "https://vendorcentral.amazon.me",
        "EG" to "https://vendorcentral.amazon.me",
        "ZA" to "https://vendorcentral.amazon.co.za",
      )

    private val SELLER_CENTRAL_URLS: Map<String, String> =
      mapOf(
        "CA" to "https://sellercentral.amazon.ca",
        "US" to "https://sellercentral.amazon.com",
        "MX" to "https://sellercentral.amazon.com.mx",
        "BR" to "https://sellercentral.amazon.com.br",
        "ES" to SELLER_EUROPE_URL,
        "UK" to SELLER_EUROPE_URL,
        "GB" to SELLER_EUROPE_URL,
        "FR" to SELLER_EUROPE_URL,
        "NL" to "https://sellercentral.amazon.nl",
        "DE" to SELLER_EUROPE_URL,
        "IT" to SELLER_EUROPE_URL,
        "SE" to "https://sellercentral.amazon.se",
        "ZA" to "https://sellercentral.amazon.co.za",
        "PL" to "https://sellercentral.amazon.pl",
        "EG" to "https://sellercentral.amazon.eg",
        "TR" to "https://sellercentral.amazon.com.tr",
        "SA" to "https://sellercentral.amazon.sa",
        "AE" to "https://sellercentral.amazon.ae",
        "IN" to "https://sellercentral.amazon.in",
        "BE" to "https://sellercentral.amazon.com.be",
        "SG" to "https://sellercentral.amazon.sg",
        "AU" to "https://sellercentral.amazon.com.au",
        "JP" to "https://sellercentral.amazon.co.jp",
      )
  }
}
