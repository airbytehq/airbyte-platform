/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.client.auth.oauth.OAuthAuthorizeTemporaryTokenUrl;
import com.google.api.client.auth.oauth.OAuthCredentialsResponse;
import com.google.api.client.auth.oauth.OAuthGetAccessToken;
import com.google.api.client.auth.oauth.OAuthGetTemporaryToken;
import com.google.api.client.auth.oauth.OAuthHmacSigner;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.oauth.BaseOAuthFlow;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Following docs from
 * https://developer.atlassian.com/cloud/trello/guides/rest-api/authorization/#using-basic-oauth.
 */
@SuppressWarnings("PMD.LooseCoupling")
public class TrelloOAuthFlow extends BaseOAuthFlow {

  private static final String REQUEST_TOKEN_URL = "https://trello.com/1/OAuthGetRequestToken";
  private static final String AUTHENTICATE_URL = "https://trello.com/1/OAuthAuthorizeToken";
  private static final String ACCESS_TOKEN_URL = "https://trello.com/1/OAuthGetAccessToken";
  private static final String OAUTH_VERIFIER = "oauth_verifier";

  // Airbyte webserver creates new TrelloOAuthFlow class instance for every API
  // call. Since oAuth 1.0 workflow requires data from previous step to build
  // correct signature.
  // Use static signer instance to share token secret for oAuth flow between
  // get_consent_url and complete_oauth API calls.
  private static final OAuthHmacSigner signer = new OAuthHmacSigner();
  private final HttpTransport transport;

  public TrelloOAuthFlow() {
    transport = new NetHttpTransport();
  }

  @VisibleForTesting
  public TrelloOAuthFlow(final HttpTransport transport) {
    this.transport = transport;
  }

  @Override
  public String getSourceConsentUrl(final UUID workspaceId,
                                    final UUID sourceDefinitionId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration,
                                    final OAuthConfigSpecification oauthConfigSpecification,
                                    JsonNode sourceOAuthParamConfig)
      throws IOException {
    return getConsentUrl(sourceOAuthParamConfig, redirectUrl);
  }

  @Override
  public String getDestinationConsentUrl(final UUID workspaceId,
                                         final UUID destinationDefinitionId,
                                         final String redirectUrl,
                                         final JsonNode inputOAuthConfiguration,
                                         final OAuthConfigSpecification oauthConfigSpecification,
                                         JsonNode destinationOAuthParamConfig)
      throws IOException {
    return getConsentUrl(destinationOAuthParamConfig, redirectUrl);
  }

  private String getConsentUrl(final JsonNode oauthParamConfig, final String redirectUrl) throws IOException {
    final String clientKey = getClientIdUnsafe(oauthParamConfig);
    final String clientSecret = getClientSecretUnsafe(oauthParamConfig);
    final OAuthGetTemporaryToken oAuthGetTemporaryToken = new OAuthGetTemporaryToken(REQUEST_TOKEN_URL);
    signer.clientSharedSecret = clientSecret;
    signer.tokenSharedSecret = null;
    oAuthGetTemporaryToken.signer = signer;
    oAuthGetTemporaryToken.callback = redirectUrl;
    oAuthGetTemporaryToken.transport = transport;
    oAuthGetTemporaryToken.consumerKey = clientKey;
    final OAuthCredentialsResponse temporaryTokenResponse = oAuthGetTemporaryToken.execute();

    final OAuthAuthorizeTemporaryTokenUrl oAuthAuthorizeTemporaryTokenUrl = new OAuthAuthorizeTemporaryTokenUrl(AUTHENTICATE_URL);
    oAuthAuthorizeTemporaryTokenUrl.temporaryToken = temporaryTokenResponse.token;
    oAuthAuthorizeTemporaryTokenUrl.set("expiration", "never");
    signer.tokenSharedSecret = temporaryTokenResponse.tokenSecret;
    return oAuthAuthorizeTemporaryTokenUrl.build();
  }

  @Override
  @Deprecated
  public Map<String, Object> completeSourceOAuth(final UUID workspaceId,
                                                 final UUID sourceDefinitionId,
                                                 final Map<String, Object> queryParams,
                                                 final String redirectUrl,
                                                 JsonNode oauthParamConfig)
      throws IOException {
    return formatOAuthOutput(oauthParamConfig, internalCompleteOAuth(oauthParamConfig, queryParams), getDefaultOAuthOutputPath());
  }

  @Override
  public Map<String, Object> completeSourceOAuth(final UUID workspaceId,
                                                 final UUID sourceDefinitionId,
                                                 final Map<String, Object> queryParams,
                                                 final String redirectUrl,
                                                 final JsonNode inputOAuthConfiguration,
                                                 final OAuthConfigSpecification oauthConfigSpecification,
                                                 final JsonNode oauthParamConfig)
      throws IOException, JsonValidationException {
    return formatOAuthOutput(oauthParamConfig, internalCompleteOAuth(oauthParamConfig, queryParams), oauthConfigSpecification);
  }

  @Override
  @Deprecated
  public Map<String, Object> completeDestinationOAuth(final UUID workspaceId,
                                                      final UUID destinationDefinitionId,
                                                      final Map<String, Object> queryParams,
                                                      final String redirectUrl,
                                                      JsonNode oauthParamConfig)
      throws IOException {
    return formatOAuthOutput(oauthParamConfig, internalCompleteOAuth(oauthParamConfig, queryParams), getDefaultOAuthOutputPath());
  }

  @Override
  public Map<String, Object> completeDestinationOAuth(final UUID workspaceId,
                                                      final UUID destinationDefinitionId,
                                                      final Map<String, Object> queryParams,
                                                      final String redirectUrl,
                                                      final JsonNode inputOAuthConfiguration,
                                                      final OAuthConfigSpecification oauthConfigSpecification,
                                                      JsonNode oauthParamConfig)
      throws IOException, JsonValidationException {
    return formatOAuthOutput(oauthParamConfig, internalCompleteOAuth(oauthParamConfig, queryParams), oauthConfigSpecification);
  }

  private Map<String, Object> internalCompleteOAuth(final JsonNode oauthParamConfig, final Map<String, Object> queryParams)
      throws IOException {
    final String clientKey = getClientIdUnsafe(oauthParamConfig);
    if (!queryParams.containsKey(OAUTH_VERIFIER) || !queryParams.containsKey("oauth_token")) {
      throw new IOException(
          "Undefined " + (!queryParams.containsKey(OAUTH_VERIFIER) ? OAUTH_VERIFIER : "oauth_token") + " from consent redirected url.");
    }
    final String temporaryToken = (String) queryParams.get("oauth_token");
    final String verificationCode = (String) queryParams.get(OAUTH_VERIFIER);
    final OAuthGetAccessToken oAuthGetAccessToken = new OAuthGetAccessToken(ACCESS_TOKEN_URL);
    oAuthGetAccessToken.signer = signer;
    oAuthGetAccessToken.transport = transport;
    oAuthGetAccessToken.temporaryToken = temporaryToken;
    oAuthGetAccessToken.verifier = verificationCode;
    oAuthGetAccessToken.consumerKey = clientKey;
    final OAuthCredentialsResponse accessTokenResponse = oAuthGetAccessToken.execute();
    final String accessToken = accessTokenResponse.token;
    return Map.of("token", accessToken, "key", clientKey);
  }

  @Override
  public List<String> getDefaultOAuthOutputPath() {
    return List.of();
  }

}
