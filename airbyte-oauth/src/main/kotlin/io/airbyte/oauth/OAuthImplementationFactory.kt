/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth

import com.google.common.collect.ImmutableMap
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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.http.HttpClient

/**
 * OAuth Implementation Factory.
 */
class OAuthImplementationFactory(
  private val httpClient: HttpClient,
) {
  private val oauthFlowMapping: Map<String, OAuthFlowImplementation>

  init {
    val builder = ImmutableMap.builder<String, OAuthFlowImplementation>()
    builder.put("airbyte/source-airtable", AirtableOAuthFlow(httpClient)) // revert me
    builder.put("airbyte/source-amazon-ads", AmazonAdsOAuthFlow(httpClient))
    builder.put("airbyte/source-amazon-seller-partner", AmazonSellerPartnerOAuthFlow(httpClient))
    builder.put("airbyte/source-asana", AsanaOAuthFlow(httpClient))
    builder.put("airbyte/source-azure-blob-storage", MicrosoftAzureBlobStorageOAuthFlow(httpClient))
    builder.put("airbyte/source-bing-ads", MicrosoftBingAdsOAuthFlow(httpClient))
    builder.put("airbyte/source-drift", DriftOAuthFlow(httpClient))
    builder.put("airbyte/source-facebook-marketing", FacebookMarketingOAuthFlow(httpClient))
    builder.put("airbyte/source-facebook-pages", FacebookPagesOAuthFlow(httpClient))
    builder.put("airbyte/source-github", GithubOAuthFlow(httpClient))
    builder.put("airbyte/source-gitlab", GitlabOAuthFlow(httpClient))
    builder.put("airbyte/source-google-ads", GoogleAdsOAuthFlow(httpClient))
    builder.put("airbyte/source-google-analytics-v4", GoogleAnalyticsViewIdOAuthFlow(httpClient))
    builder.put("airbyte/source-google-analytics-data-api", GoogleAnalyticsPropertyIdOAuthFlow(httpClient))
    builder.put("airbyte/source-gcs", GoogleCloudStorageOAuthFlow(httpClient))
    builder.put("airbyte/source-google-search-console", GoogleSearchConsoleOAuthFlow(httpClient))
    builder.put("airbyte/source-google-sheets", GoogleSheetsOAuthFlow(httpClient))
    builder.put("airbyte/source-google-drive", GoogleDriveOAuthFlow(httpClient))
    builder.put("airbyte/source-harvest", HarvestOAuthFlow(httpClient))
    builder.put("airbyte/source-hubspot", HubspotOAuthFlow(httpClient))
    builder.put("airbyte/source-intercom", IntercomOAuthFlow(httpClient))
    builder.put("airbyte/source-instagram", InstagramOAuthFlow(httpClient))
    builder.put("airbyte/source-lever-hiring", LeverOAuthFlow(httpClient))
    builder.put("airbyte/source-linkedin-ads", LinkedinAdsOAuthFlow(httpClient))
    builder.put("airbyte/source-mailchimp", MailchimpOAuthFlow(httpClient))
    builder.put("airbyte/source-microsoft-teams", MicrosoftTeamsOAuthFlow(httpClient))
    builder.put("airbyte/source-microsoft-onedrive", MicrosoftOneDriveOAuthFlow(httpClient))
    builder.put("airbyte/source-microsoft-sharepoint", MicrosoftSharepointOAuthFlow(httpClient))
    builder.put("airbyte/source-monday", MondayOAuthFlow(httpClient))
    builder.put("airbyte/source-notion", NotionOAuthFlow(httpClient))
    builder.put("airbyte/source-okta", OktaOAuthFlow(httpClient))
    builder.put("airbyte/source-paypal-transaction", PayPalTransactionOAuthFlow(httpClient))
    builder.put("airbyte/source-pinterest", PinterestOAuthFlow(httpClient))
    builder.put("airbyte/source-pipedrive", PipeDriveOAuthFlow(httpClient))
    builder.put("airbyte/source-quickbooks", QuickbooksOAuthFlow(httpClient))
    builder.put("airbyte/source-retently", RetentlyOAuthFlow(httpClient))
    builder.put("airbyte/source-salesforce", SalesforceOAuthFlow(httpClient))
    builder.put("airbyte/source-shopify", ShopifyOAuthFlow(httpClient))
    builder.put("airbyte/source-slack", SlackOAuthFlow(httpClient))
    builder.put("airbyte/source-smartsheets", SmartsheetsOAuthFlow(httpClient))
    builder.put("airbyte/source-snapchat-marketing", SnapchatMarketingOAuthFlow(httpClient))
    builder.put("airbyte/source-snowflake", SourceSnowflakeOAuthFlow(httpClient))
    builder.put("airbyte/source-square", SquareOAuthFlow(httpClient))
    builder.put("airbyte/source-strava", StravaOAuthFlow(httpClient))
    builder.put("airbyte/source-surveymonkey", SurveymonkeyOAuthFlow(httpClient))
    builder.put("airbyte/source-tiktok-marketing", TikTokMarketingOAuthFlow(httpClient))
    builder.put("airbyte/source-trello", TrelloOAuthFlow())
    builder.put("airbyte/source-typeform", TypeformOAuthFlow(httpClient))
    builder.put("airbyte/source-youtube-analytics", YouTubeAnalyticsOAuthFlow(httpClient))
    builder.put("airbyte/source-xero", XeroOAuthFlow(httpClient))
    builder.put("airbyte/source-zendesk-chat", ZendeskChatOAuthFlow(httpClient))
    builder.put("airbyte/source-zendesk-sunshine", ZendeskSunshineOAuthFlow(httpClient))
    builder.put("airbyte/source-zendesk-support", ZendeskSupportOAuthFlow(httpClient))
    builder.put("airbyte/source-zendesk-talk", ZendeskTalkOAuthFlow(httpClient))
    builder.put("airbyte/destination-snowflake", DestinationSnowflakeOAuthFlow(httpClient))
    builder.put("airbyte/destination-google-sheets", DestinationGoogleSheetsOAuthFlow(httpClient))
    builder.put("airbyte/destination-cobra", SalesforceOAuthFlow(httpClient))
    oauthFlowMapping = builder.build()
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
    } catch (e: IllegalStateException) {
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
      LOGGER.info("Using {} for {}", oauthFlowMapping[imageName]!!.javaClass.simpleName, imageName)
      return oauthFlowMapping[imageName]
    } else {
      throw IllegalStateException(
        String.format("Requested OAuth implementation for %s, but it is not included in the oauth mapping.", imageName),
      )
    }
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(OAuthImplementationFactory::class.java)

    private fun hasDeclarativeOAuthConfigSpecification(spec: ConnectorSpecification?): Boolean =
      spec != null &&
        spec.advancedAuth != null &&
        spec.advancedAuth.oauthConfigSpecification != null &&
        spec.advancedAuth.oauthConfigSpecification.oauthConnectorInputSpecification != null
  }
}
