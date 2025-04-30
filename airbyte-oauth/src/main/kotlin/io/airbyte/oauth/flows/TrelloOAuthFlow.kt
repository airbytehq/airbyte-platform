/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.client.auth.oauth.OAuthAuthorizeTemporaryTokenUrl
import com.google.api.client.auth.oauth.OAuthGetAccessToken
import com.google.api.client.auth.oauth.OAuthGetTemporaryToken
import com.google.api.client.auth.oauth.OAuthHmacSigner
import com.google.api.client.http.HttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.common.annotations.VisibleForTesting
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.UUID

/**
 * Following docs from
 * https://developer.atlassian.com/cloud/trello/guides/rest-api/authorization/#using-basic-oauth.
 */
class TrelloOAuthFlow : BaseOAuthFlow {
  private val transport: HttpTransport

  constructor() {
    transport = NetHttpTransport()
  }

  @VisibleForTesting
  constructor(transport: HttpTransport) {
    this.transport = transport
  }

  @Throws(IOException::class)
  override fun getSourceConsentUrl(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification?,
    sourceOAuthParamConfig: JsonNode?,
  ): String = getConsentUrl(sourceOAuthParamConfig!!, redirectUrl)

  @Throws(IOException::class)
  override fun getDestinationConsentUrl(
    workspaceId: UUID,
    destinationDefinitionId: UUID?,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification?,
    destinationOAuthParamConfig: JsonNode?,
  ): String = getConsentUrl(destinationOAuthParamConfig!!, redirectUrl)

  @Throws(IOException::class)
  private fun getConsentUrl(
    oauthParamConfig: JsonNode,
    redirectUrl: String,
  ): String {
    val clientKey = getClientIdUnsafe(oauthParamConfig)
    val clientSecret = getClientSecretUnsafe(oauthParamConfig)
    val oAuthGetTemporaryToken = OAuthGetTemporaryToken(REQUEST_TOKEN_URL)
    signer.clientSharedSecret = clientSecret
    signer.tokenSharedSecret = null
    oAuthGetTemporaryToken.signer = signer
    oAuthGetTemporaryToken.callback = redirectUrl
    oAuthGetTemporaryToken.transport = transport
    oAuthGetTemporaryToken.consumerKey = clientKey
    val temporaryTokenResponse = oAuthGetTemporaryToken.execute()

    val oAuthAuthorizeTemporaryTokenUrl = OAuthAuthorizeTemporaryTokenUrl(AUTHENTICATE_URL)
    oAuthAuthorizeTemporaryTokenUrl.temporaryToken = temporaryTokenResponse.token
    oAuthAuthorizeTemporaryTokenUrl["expiration"] = "never"
    signer.tokenSharedSecret = temporaryTokenResponse.tokenSecret
    return oAuthAuthorizeTemporaryTokenUrl.build()
  }

  @Deprecated("")
  @Throws(IOException::class)
  override fun completeSourceOAuth(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> = formatOAuthOutput(oauthParamConfig, internalCompleteOAuth(oauthParamConfig, queryParams), getDefaultOAuthOutputPath())

  @Throws(IOException::class, JsonValidationException::class)
  override fun completeSourceOAuth(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> = formatOAuthOutput(oauthParamConfig, internalCompleteOAuth(oauthParamConfig, queryParams), oauthConfigSpecification)

  @Deprecated("")
  @Throws(IOException::class)
  override fun completeDestinationOAuth(
    workspaceId: UUID,
    destinationDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> = formatOAuthOutput(oauthParamConfig, internalCompleteOAuth(oauthParamConfig, queryParams), getDefaultOAuthOutputPath())

  @Throws(IOException::class, JsonValidationException::class)
  override fun completeDestinationOAuth(
    workspaceId: UUID,
    destinationDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> = formatOAuthOutput(oauthParamConfig, internalCompleteOAuth(oauthParamConfig, queryParams), oauthConfigSpecification)

  @Throws(IOException::class)
  private fun internalCompleteOAuth(
    oauthParamConfig: JsonNode,
    queryParams: Map<String, Any>,
  ): Map<String, Any> {
    val clientKey = getClientIdUnsafe(oauthParamConfig)
    if (!queryParams.containsKey(OAUTH_VERIFIER) || !queryParams.containsKey("oauth_token")) {
      throw IOException(
        "Undefined " + (if (!queryParams.containsKey(OAUTH_VERIFIER)) OAUTH_VERIFIER else "oauth_token") + " from consent redirected url.",
      )
    }
    val temporaryToken = queryParams["oauth_token"] as String?
    val verificationCode = queryParams[OAUTH_VERIFIER] as String?
    val oAuthGetAccessToken = OAuthGetAccessToken(ACCESS_TOKEN_URL)
    oAuthGetAccessToken.signer = signer
    oAuthGetAccessToken.transport = transport
    oAuthGetAccessToken.temporaryToken = temporaryToken
    oAuthGetAccessToken.verifier = verificationCode
    oAuthGetAccessToken.consumerKey = clientKey
    val accessTokenResponse = oAuthGetAccessToken.execute()
    val accessToken = accessTokenResponse.token
    return java.util.Map.of<String, Any>("token", accessToken, "key", clientKey)
  }

  override fun getDefaultOAuthOutputPath(): List<String> = listOf()

  companion object {
    private const val REQUEST_TOKEN_URL = "https://trello.com/1/OAuthGetRequestToken"
    private const val AUTHENTICATE_URL = "https://trello.com/1/OAuthAuthorizeToken"
    private const val ACCESS_TOKEN_URL = "https://trello.com/1/OAuthGetAccessToken"
    private const val OAUTH_VERIFIER = "oauth_verifier"

    // Airbyte webserver creates new TrelloOAuthFlow class instance for every API
    // call. Since oAuth 1.0 workflow requires data from previous step to build
    // correct signature.
    // Use static signer instance to share token secret for oAuth flow between
    // get_consent_url and complete_oauth API calls.
    private val signer = OAuthHmacSigner()
  }
}
