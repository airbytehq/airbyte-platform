/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.oauth.BaseOAuth2Flow
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
  @Throws(IOException::class, JsonValidationException::class)
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
        .addParameter("redirect_uri", redirectUrl)
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

  @Throws(IOException::class)
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
    ImmutableMap
      .builder<String, String>() // required
      .put("client_id", clientId)
      .put("redirect_uri", redirectUrl)
      .put("client_secret", clientSecret)
      .put("code", authCode)
      .put("grant_type", "authorization_code")
      .build()

  /**
   * Returns the URL where to retrieve the access token from.
   *
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun getDefaultOAuthOutputPath(): List<String> = listOf()

    /*
     * Should be overriden to satisfy the BaseOAuth2Flow abstraction requirements
     */
  @Throws(IOException::class)
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
      ImmutableMap
        .builder<String, String>()
        .put("CA", "https://vendorcentral.amazon.ca")
        .put("US", "https://vendorcentral.amazon.com")
        .put("DE", "https://vendorcentral.amazon.de")
        .put("MX", "https://vendorcentral.amazon.com.mx")
        .put("JP", "https://vendorcentral.amazon.co.jp")
        .put("IT", "https://vendorcentral.amazon.it")
        .put("AU", "https://vendorcentral.amazon.com.au")
        .put("BR", "https://vendorcentral.amazon.com.br")
        .put("BE", "https://vendorcentral.amazon.com.be")
        .put("ES", "https://vendorcentral.amazon.es")
        .put("UK", "https://vendorcentral.amazon.co.uk")
        .put("GB", "https://vendorcentral.amazon.co.uk")
        .put("NL", "https://vendorcentral.amazon.nl")
        .put("PL", "https://vendorcentral.amazon.pl")
        .put("FR", "https://vendorcentral.amazon.fr")
        .put("IN", "https://www.vendorcentral.in")
        .put("SE", "https://vendorcentral.amazon.se")
        .put("SG", "https://vendorcentral.amazon.com.sg")
        .put("AE", "https://vendorcentral.amazon.me")
        .put("TR", "https://vendorcentral.amazon.com.tr")
        .put("SA", "https://vendorcentral.amazon.me")
        .put("EG", "https://vendorcentral.amazon.me")
        .put("ZA", "https://vendorcentral.amazon.co.za")
        .build()

    private val SELLER_CENTRAL_URLS: Map<String, String> =
      ImmutableMap
        .builder<String, String>()
        .put("CA", "https://sellercentral.amazon.ca")
        .put("US", "https://sellercentral.amazon.com")
        .put("MX", "https://sellercentral.amazon.com.mx")
        .put("BR", "https://sellercentral.amazon.com.br")
        .put("ES", SELLER_EUROPE_URL)
        .put("UK", SELLER_EUROPE_URL)
        .put("GB", SELLER_EUROPE_URL)
        .put("FR", SELLER_EUROPE_URL)
        .put("NL", "https://sellercentral.amazon.nl")
        .put("DE", SELLER_EUROPE_URL)
        .put("IT", SELLER_EUROPE_URL)
        .put("SE", "https://sellercentral.amazon.se")
        .put("ZA", "https://sellercentral.amazon.co.za")
        .put("PL", "https://sellercentral.amazon.pl")
        .put("EG", "https://sellercentral.amazon.eg")
        .put("TR", "https://sellercentral.amazon.com.tr")
        .put("SA", "https://sellercentral.amazon.sa")
        .put("AE", "https://sellercentral.amazon.ae")
        .put("IN", "https://sellercentral.amazon.in")
        .put("BE", "https://sellercentral.amazon.com.be")
        .put("SG", "https://sellercentral.amazon.sg")
        .put("AU", "https://sellercentral.amazon.com.au")
        .put("JP", "https://sellercentral.amazon.co.jp")
        .build()
  }
}
