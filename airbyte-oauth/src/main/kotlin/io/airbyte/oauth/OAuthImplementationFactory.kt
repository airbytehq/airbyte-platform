/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth

import io.airbyte.oauth.declarative.DeclarativeOAuthFlow
import io.airbyte.oauth.flows.AirtableOAuthFlow
import io.airbyte.oauth.flows.AmazonAdsOAuthFlow
import io.airbyte.oauth.flows.AmazonSellerPartnerOAuthFlow
import io.airbyte.oauth.flows.AsanaOAuthFlow
import io.airbyte.oauth.flows.DestinationSnowflakeOAuthFlow
import io.airbyte.oauth.flows.DriftOAuthFlow
import io.airbyte.oauth.flows.GithubOAuthFlow
import io.airbyte.oauth.flows.GitlabOAuthFlow
import io.airbyte.oauth.flows.HarvestOAuthFlow
import io.airbyte.oauth.flows.HubspotOAuthFlow
import io.airbyte.oauth.flows.IntercomOAuthFlow
import io.airbyte.oauth.flows.LeverOAuthFlow
import io.airbyte.oauth.flows.LinkedinAdsOAuthFlow
import io.airbyte.oauth.flows.MailchimpOAuthFlow
import io.airbyte.oauth.flows.MicrosoftAzureBlobStorageOAuthFlow
import io.airbyte.oauth.flows.MicrosoftBingAdsOAuthFlow
import io.airbyte.oauth.flows.MicrosoftOneDriveOAuthFlow
import io.airbyte.oauth.flows.MicrosoftSharepointOAuthFlow
import io.airbyte.oauth.flows.MicrosoftTeamsOAuthFlow
import io.airbyte.oauth.flows.MondayOAuthFlow
import io.airbyte.oauth.flows.NotionOAuthFlow
import io.airbyte.oauth.flows.OktaOAuthFlow
import io.airbyte.oauth.flows.PayPalTransactionOAuthFlow
import io.airbyte.oauth.flows.PinterestOAuthFlow
import io.airbyte.oauth.flows.PipeDriveOAuthFlow
import io.airbyte.oauth.flows.QuickbooksOAuthFlow
import io.airbyte.oauth.flows.RetentlyOAuthFlow
import io.airbyte.oauth.flows.SalesforceOAuthFlow
import io.airbyte.oauth.flows.ShopifyOAuthFlow
import io.airbyte.oauth.flows.SlackOAuthFlow
import io.airbyte.oauth.flows.SmartsheetsOAuthFlow
import io.airbyte.oauth.flows.SnapchatMarketingOAuthFlow
import io.airbyte.oauth.flows.SourceSnowflakeOAuthFlow
import io.airbyte.oauth.flows.SquareOAuthFlow
import io.airbyte.oauth.flows.StravaOAuthFlow
import io.airbyte.oauth.flows.SurveymonkeyOAuthFlow
import io.airbyte.oauth.flows.TikTokMarketingOAuthFlow
import io.airbyte.oauth.flows.TrelloOAuthFlow
import io.airbyte.oauth.flows.TypeformOAuthFlow
import io.airbyte.oauth.flows.XeroOAuthFlow
import io.airbyte.oauth.flows.ZendeskChatOAuthFlow
import io.airbyte.oauth.flows.ZendeskSunshineOAuthFlow
import io.airbyte.oauth.flows.ZendeskSupportOAuthFlow
import io.airbyte.oauth.flows.ZendeskTalkOAuthFlow
import io.airbyte.oauth.flows.facebook.FacebookMarketingOAuthFlow
import io.airbyte.oauth.flows.facebook.FacebookPagesOAuthFlow
import io.airbyte.oauth.flows.facebook.InstagramOAuthFlow
import io.airbyte.oauth.flows.google.DestinationGoogleSheetsOAuthFlow
import io.airbyte.oauth.flows.google.GoogleAdsOAuthFlow
import io.airbyte.oauth.flows.google.GoogleAnalyticsPropertyIdOAuthFlow
import io.airbyte.oauth.flows.google.GoogleAnalyticsViewIdOAuthFlow
import io.airbyte.oauth.flows.google.GoogleCloudStorageOAuthFlow
import io.airbyte.oauth.flows.google.GoogleDriveOAuthFlow
import io.airbyte.oauth.flows.google.GoogleSearchConsoleOAuthFlow
import io.airbyte.oauth.flows.google.GoogleSheetsOAuthFlow
import io.airbyte.oauth.flows.google.YouTubeAnalyticsOAuthFlow
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.http.HttpClient

private val log = KotlinLogging.logger {}

/**
 * OAuth Implementation Factory.
 */
class OAuthImplementationFactory(
  private val httpClient: HttpClient,
) {
  private val oauthFlowMapping: Map<String, OAuthFlowImplementation>

  init {
    oauthFlowMapping =
      mapOf(
        "airbyte/source-airtable" to AirtableOAuthFlow(httpClient), // revert me
        "airbyte/source-amazon-ads" to AmazonAdsOAuthFlow(httpClient),
        "airbyte/source-amazon-seller-partner" to AmazonSellerPartnerOAuthFlow(httpClient),
        "airbyte/source-asana" to AsanaOAuthFlow(httpClient),
        "airbyte/source-azure-blob-storage" to MicrosoftAzureBlobStorageOAuthFlow(httpClient),
        "airbyte/source-bing-ads" to MicrosoftBingAdsOAuthFlow(httpClient),
        "airbyte/source-drift" to DriftOAuthFlow(httpClient),
        "airbyte/source-facebook-marketing" to FacebookMarketingOAuthFlow(httpClient),
        "airbyte/source-facebook-pages" to FacebookPagesOAuthFlow(httpClient),
        "airbyte/source-github" to GithubOAuthFlow(httpClient),
        "airbyte/source-gitlab" to GitlabOAuthFlow(httpClient),
        "airbyte/source-google-ads" to GoogleAdsOAuthFlow(httpClient),
        "airbyte/source-google-analytics-v4" to GoogleAnalyticsViewIdOAuthFlow(httpClient),
        "airbyte/source-google-analytics-data-api" to GoogleAnalyticsPropertyIdOAuthFlow(httpClient),
        "airbyte/source-gcs" to GoogleCloudStorageOAuthFlow(httpClient),
        "airbyte/source-google-search-console" to GoogleSearchConsoleOAuthFlow(httpClient),
        "airbyte/source-google-sheets" to GoogleSheetsOAuthFlow(httpClient),
        "airbyte/source-google-drive" to GoogleDriveOAuthFlow(httpClient),
        "airbyte/source-harvest" to HarvestOAuthFlow(httpClient),
        "airbyte/source-hubspot" to HubspotOAuthFlow(httpClient),
        "airbyte/source-intercom" to IntercomOAuthFlow(httpClient),
        "airbyte/source-instagram" to InstagramOAuthFlow(httpClient),
        "airbyte/source-lever-hiring" to LeverOAuthFlow(httpClient),
        "airbyte/source-linkedin-ads" to LinkedinAdsOAuthFlow(httpClient),
        "airbyte/source-mailchimp" to MailchimpOAuthFlow(httpClient),
        "airbyte/source-microsoft-teams" to MicrosoftTeamsOAuthFlow(httpClient),
        "airbyte/source-microsoft-onedrive" to MicrosoftOneDriveOAuthFlow(httpClient),
        "airbyte/source-microsoft-sharepoint" to MicrosoftSharepointOAuthFlow(httpClient),
        "airbyte/source-monday" to MondayOAuthFlow(httpClient),
        "airbyte/source-notion" to NotionOAuthFlow(httpClient),
        "airbyte/source-okta" to OktaOAuthFlow(httpClient),
        "airbyte/source-paypal-transaction" to PayPalTransactionOAuthFlow(httpClient),
        "airbyte/source-pinterest" to PinterestOAuthFlow(httpClient),
        "airbyte/source-pipedrive" to PipeDriveOAuthFlow(httpClient),
        "airbyte/source-quickbooks" to QuickbooksOAuthFlow(httpClient),
        "airbyte/source-retently" to RetentlyOAuthFlow(httpClient),
        "airbyte/source-salesforce" to SalesforceOAuthFlow(httpClient),
        "airbyte/source-shopify" to ShopifyOAuthFlow(httpClient),
        "airbyte/source-slack" to SlackOAuthFlow(httpClient),
        "airbyte/source-smartsheets" to SmartsheetsOAuthFlow(httpClient),
        "airbyte/source-snapchat-marketing" to SnapchatMarketingOAuthFlow(httpClient),
        "airbyte/source-snowflake" to SourceSnowflakeOAuthFlow(httpClient),
        "airbyte/source-square" to SquareOAuthFlow(httpClient),
        "airbyte/source-strava" to StravaOAuthFlow(httpClient),
        "airbyte/source-surveymonkey" to SurveymonkeyOAuthFlow(httpClient),
        "airbyte/source-tiktok-marketing" to TikTokMarketingOAuthFlow(httpClient),
        "airbyte/source-trello" to TrelloOAuthFlow(),
        "airbyte/source-typeform" to TypeformOAuthFlow(httpClient),
        "airbyte/source-youtube-analytics" to YouTubeAnalyticsOAuthFlow(httpClient),
        "airbyte/source-xero" to XeroOAuthFlow(httpClient),
        "airbyte/source-zendesk-chat" to ZendeskChatOAuthFlow(httpClient),
        "airbyte/source-zendesk-sunshine" to ZendeskSunshineOAuthFlow(httpClient),
        "airbyte/source-zendesk-support" to ZendeskSupportOAuthFlow(httpClient),
        "airbyte/source-zendesk-talk" to ZendeskTalkOAuthFlow(httpClient),
        "airbyte/destination-snowflake" to DestinationSnowflakeOAuthFlow(httpClient),
        "airbyte/destination-google-sheets" to DestinationGoogleSheetsOAuthFlow(httpClient),
        "airbyte/destination-cobra" to SalesforceOAuthFlow(httpClient),
        "airbyte/destination-salesforce" to SalesforceOAuthFlow(httpClient),
      )
  }

  /**
   * Returns the OAuthFlowImplementation for a given source or destination, preferring the declarative
   * OAuth flow if declared in the connector's spec, and falling back to specific implementations
   * otherwise.
   *
   * @param imageName - docker repository name for the connector
   * @param connectorSpecification - the spec for the connector
   * @return OAuthFlowImplementation
   */
  fun create(
    imageName: String,
    connectorSpecification: ConnectorSpecification?,
  ): OAuthFlowImplementation? =
    try {
      createDeclarativeOAuthImplementation(connectorSpecification)
    } catch (_: IllegalStateException) {
      createNonDeclarativeOAuthImplementation(imageName)
    }

  /**
   * Creates a DeclarativeOAuthFlow for a given connector spec.
   *
   * @param connectorSpecification - the spec for the connector
   * @return DeclarativeOAuthFlow
   */
  fun createDeclarativeOAuthImplementation(connectorSpecification: ConnectorSpecification?): DeclarativeOAuthFlow {
    check(hasDeclarativeOAuthConfigSpecification(connectorSpecification)) {
      "Cannot create DeclarativeOAuthFlow without a declarative OAuth config spec."
    }
    return DeclarativeOAuthFlow(httpClient)
  }

  private fun createNonDeclarativeOAuthImplementation(imageName: String): OAuthFlowImplementation? {
    if (oauthFlowMapping.containsKey(imageName)) {
      log.info { "Using ${oauthFlowMapping[imageName]!!.javaClass.simpleName} for $imageName" }
      return oauthFlowMapping[imageName]
    } else {
      throw IllegalStateException(
        String.format("Requested OAuth implementation for %s, but it is not included in the oauth mapping.", imageName),
      )
    }
  }

  companion object {
    private fun hasDeclarativeOAuthConfigSpecification(spec: ConnectorSpecification?): Boolean =
      spec != null &&
        spec.advancedAuth != null &&
        spec.advancedAuth.oauthConfigSpecification != null &&
        spec.advancedAuth.oauthConfigSpecification.oauthConnectorInputSpecification != null
  }
}
