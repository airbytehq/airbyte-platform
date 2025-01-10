/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airbyte.oauth.declarative.DeclarativeOAuthFlow;
import io.airbyte.oauth.flows.AirtableOAuthFlow;
import io.airbyte.oauth.flows.AmazonAdsOAuthFlow;
import io.airbyte.oauth.flows.AmazonSellerPartnerOAuthFlow;
import io.airbyte.oauth.flows.AsanaOAuthFlow;
import io.airbyte.oauth.flows.DestinationSnowflakeOAuthFlow;
import io.airbyte.oauth.flows.DriftOAuthFlow;
import io.airbyte.oauth.flows.GithubOAuthFlow;
import io.airbyte.oauth.flows.GitlabOAuthFlow;
import io.airbyte.oauth.flows.HarvestOAuthFlow;
import io.airbyte.oauth.flows.HubspotOAuthFlow;
import io.airbyte.oauth.flows.IntercomOAuthFlow;
import io.airbyte.oauth.flows.LeverOAuthFlow;
import io.airbyte.oauth.flows.LinkedinAdsOAuthFlow;
import io.airbyte.oauth.flows.MailchimpOAuthFlow;
import io.airbyte.oauth.flows.MicrosoftAzureBlobStorageOAuthFlow;
import io.airbyte.oauth.flows.MicrosoftBingAdsOAuthFlow;
import io.airbyte.oauth.flows.MicrosoftOneDriveOAuthFlow;
import io.airbyte.oauth.flows.MicrosoftSharepointOAuthFlow;
import io.airbyte.oauth.flows.MicrosoftTeamsOAuthFlow;
import io.airbyte.oauth.flows.MondayOAuthFlow;
import io.airbyte.oauth.flows.NotionOAuthFlow;
import io.airbyte.oauth.flows.OktaOAuthFlow;
import io.airbyte.oauth.flows.PayPalTransactionOAuthFlow;
import io.airbyte.oauth.flows.PinterestOAuthFlow;
import io.airbyte.oauth.flows.PipeDriveOAuthFlow;
import io.airbyte.oauth.flows.QuickbooksOAuthFlow;
import io.airbyte.oauth.flows.RetentlyOAuthFlow;
import io.airbyte.oauth.flows.SalesforceOAuthFlow;
import io.airbyte.oauth.flows.ShopifyOAuthFlow;
import io.airbyte.oauth.flows.SlackOAuthFlow;
import io.airbyte.oauth.flows.SmartsheetsOAuthFlow;
import io.airbyte.oauth.flows.SnapchatMarketingOAuthFlow;
import io.airbyte.oauth.flows.SourceSnowflakeOAuthFlow;
import io.airbyte.oauth.flows.SquareOAuthFlow;
import io.airbyte.oauth.flows.StravaOAuthFlow;
import io.airbyte.oauth.flows.SurveymonkeyOAuthFlow;
import io.airbyte.oauth.flows.TikTokMarketingOAuthFlow;
import io.airbyte.oauth.flows.TrelloOAuthFlow;
import io.airbyte.oauth.flows.TypeformOAuthFlow;
import io.airbyte.oauth.flows.XeroOAuthFlow;
import io.airbyte.oauth.flows.ZendeskChatOAuthFlow;
import io.airbyte.oauth.flows.ZendeskSunshineOAuthFlow;
import io.airbyte.oauth.flows.ZendeskSupportOAuthFlow;
import io.airbyte.oauth.flows.ZendeskTalkOAuthFlow;
import io.airbyte.oauth.flows.facebook.FacebookMarketingOAuthFlow;
import io.airbyte.oauth.flows.facebook.FacebookPagesOAuthFlow;
import io.airbyte.oauth.flows.facebook.InstagramOAuthFlow;
import io.airbyte.oauth.flows.google.DestinationGoogleSheetsOAuthFlow;
import io.airbyte.oauth.flows.google.GoogleAdsOAuthFlow;
import io.airbyte.oauth.flows.google.GoogleAnalyticsPropertyIdOAuthFlow;
import io.airbyte.oauth.flows.google.GoogleAnalyticsViewIdOAuthFlow;
import io.airbyte.oauth.flows.google.GoogleCloudStorageOAuthFlow;
import io.airbyte.oauth.flows.google.GoogleDriveOAuthFlow;
import io.airbyte.oauth.flows.google.GoogleSearchConsoleOAuthFlow;
import io.airbyte.oauth.flows.google.GoogleSheetsOAuthFlow;
import io.airbyte.oauth.flows.google.YouTubeAnalyticsOAuthFlow;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.net.http.HttpClient;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth Implementation Factory.
 */
public class OAuthImplementationFactory {

  private final Map<String, OAuthFlowImplementation> oauthFlowMapping;
  private final HttpClient httpClient;
  private static final Logger LOGGER = LoggerFactory.getLogger(OAuthImplementationFactory.class);

  public OAuthImplementationFactory(final HttpClient httpClient) {
    this.httpClient = httpClient;
    final Builder<String, OAuthFlowImplementation> builder = ImmutableMap.builder();
    builder.put("airbyte/source-airtable", new AirtableOAuthFlow(httpClient)); // revert me
    builder.put("airbyte/source-amazon-ads", new AmazonAdsOAuthFlow(httpClient));
    builder.put("airbyte/source-amazon-seller-partner", new AmazonSellerPartnerOAuthFlow(httpClient));
    builder.put("airbyte/source-asana", new AsanaOAuthFlow(httpClient));
    builder.put("airbyte/source-azure-blob-storage", new MicrosoftAzureBlobStorageOAuthFlow(httpClient));
    builder.put("airbyte/source-bing-ads", new MicrosoftBingAdsOAuthFlow(httpClient));
    builder.put("airbyte/source-drift", new DriftOAuthFlow(httpClient));
    builder.put("airbyte/source-facebook-marketing", new FacebookMarketingOAuthFlow(httpClient));
    builder.put("airbyte/source-facebook-pages", new FacebookPagesOAuthFlow(httpClient));
    builder.put("airbyte/source-github", new GithubOAuthFlow(httpClient));
    builder.put("airbyte/source-gitlab", new GitlabOAuthFlow(httpClient));
    builder.put("airbyte/source-google-ads", new GoogleAdsOAuthFlow(httpClient));
    builder.put("airbyte/source-google-analytics-v4", new GoogleAnalyticsViewIdOAuthFlow(httpClient));
    builder.put("airbyte/source-google-analytics-data-api", new GoogleAnalyticsPropertyIdOAuthFlow(httpClient));
    builder.put("airbyte/source-gcs", new GoogleCloudStorageOAuthFlow(httpClient));
    builder.put("airbyte/source-google-search-console", new GoogleSearchConsoleOAuthFlow(httpClient));
    builder.put("airbyte/source-google-sheets", new GoogleSheetsOAuthFlow(httpClient));
    builder.put("airbyte/source-google-drive", new GoogleDriveOAuthFlow(httpClient));
    builder.put("airbyte/source-harvest", new HarvestOAuthFlow(httpClient));
    builder.put("airbyte/source-hubspot", new HubspotOAuthFlow(httpClient));
    builder.put("airbyte/source-intercom", new IntercomOAuthFlow(httpClient));
    builder.put("airbyte/source-instagram", new InstagramOAuthFlow(httpClient));
    builder.put("airbyte/source-lever-hiring", new LeverOAuthFlow(httpClient));
    builder.put("airbyte/source-linkedin-ads", new LinkedinAdsOAuthFlow(httpClient));
    builder.put("airbyte/source-mailchimp", new MailchimpOAuthFlow(httpClient));
    builder.put("airbyte/source-microsoft-teams", new MicrosoftTeamsOAuthFlow(httpClient));
    builder.put(
        "airbyte/source-microsoft-onedrive", new MicrosoftOneDriveOAuthFlow(httpClient));
    builder.put(
        "airbyte/source-microsoft-sharepoint", new MicrosoftSharepointOAuthFlow(httpClient));
    builder.put("airbyte/source-monday", new MondayOAuthFlow(httpClient));
    builder.put("airbyte/source-notion", new NotionOAuthFlow(httpClient));
    builder.put("airbyte/source-okta", new OktaOAuthFlow(httpClient));
    builder.put("airbyte/source-paypal-transaction", new PayPalTransactionOAuthFlow(httpClient));
    builder.put("airbyte/source-pinterest", new PinterestOAuthFlow(httpClient));
    builder.put("airbyte/source-pipedrive", new PipeDriveOAuthFlow(httpClient));
    builder.put("airbyte/source-quickbooks", new QuickbooksOAuthFlow(httpClient));
    builder.put("airbyte/source-retently", new RetentlyOAuthFlow(httpClient));
    builder.put("airbyte/source-salesforce", new SalesforceOAuthFlow(httpClient));
    builder.put("airbyte/source-shopify", new ShopifyOAuthFlow(httpClient));
    builder.put("airbyte/source-slack", new SlackOAuthFlow(httpClient));
    builder.put("airbyte/source-smartsheets", new SmartsheetsOAuthFlow(httpClient));
    builder.put("airbyte/source-snapchat-marketing", new SnapchatMarketingOAuthFlow(httpClient));
    builder.put("airbyte/source-snowflake", new SourceSnowflakeOAuthFlow(httpClient));
    builder.put("airbyte/source-square", new SquareOAuthFlow(httpClient));
    builder.put("airbyte/source-strava", new StravaOAuthFlow(httpClient));
    builder.put("airbyte/source-surveymonkey", new SurveymonkeyOAuthFlow(httpClient));
    builder.put("airbyte/source-tiktok-marketing", new TikTokMarketingOAuthFlow(httpClient));
    builder.put("airbyte/source-trello", new TrelloOAuthFlow());
    builder.put("airbyte/source-typeform", new TypeformOAuthFlow(httpClient));
    builder.put("airbyte/source-youtube-analytics", new YouTubeAnalyticsOAuthFlow(httpClient));
    builder.put("airbyte/source-xero", new XeroOAuthFlow(httpClient));
    builder.put("airbyte/source-zendesk-chat", new ZendeskChatOAuthFlow(httpClient));
    builder.put("airbyte/source-zendesk-sunshine", new ZendeskSunshineOAuthFlow(httpClient));
    builder.put("airbyte/source-zendesk-support", new ZendeskSupportOAuthFlow(httpClient));
    builder.put("airbyte/source-zendesk-talk", new ZendeskTalkOAuthFlow(httpClient));
    builder.put("airbyte/destination-snowflake", new DestinationSnowflakeOAuthFlow(httpClient));
    builder.put("airbyte/destination-google-sheets", new DestinationGoogleSheetsOAuthFlow(httpClient));
    oauthFlowMapping = builder.build();
  }

  private static boolean hasDeclarativeOAuthConfigSpecification(final ConnectorSpecification spec) {
    return spec != null && spec.getAdvancedAuth() != null && spec.getAdvancedAuth().getOauthConfigSpecification() != null
        && spec.getAdvancedAuth().getOauthConfigSpecification().getOauthConnectorInputSpecification() != null;
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
  public OAuthFlowImplementation create(final String imageName, final ConnectorSpecification connectorSpecification) {
    try {
      return createDeclarativeOAuthImplementation(connectorSpecification);
    } catch (final IllegalStateException e) {
      return createNonDeclarativeOAuthImplementation(imageName);
    }
  }

  /**
   * Creates a DeclarativeOAuthFlow for a given connector spec.
   *
   * @param connectorSpecification - the spec for the connector
   * @return DeclarativeOAuthFlow
   */
  public DeclarativeOAuthFlow createDeclarativeOAuthImplementation(final ConnectorSpecification connectorSpecification) {
    if (!hasDeclarativeOAuthConfigSpecification(connectorSpecification)) {
      throw new IllegalStateException("Cannot create DeclarativeOAuthFlow without a declarative OAuth config spec.");
    }
    return new DeclarativeOAuthFlow(httpClient);
  }

  private OAuthFlowImplementation createNonDeclarativeOAuthImplementation(final String imageName) {
    if (oauthFlowMapping.containsKey(imageName)) {
      LOGGER.info("Using {} for {}", oauthFlowMapping.get(imageName).getClass().getSimpleName(), imageName);
      return oauthFlowMapping.get(imageName);
    } else {
      throw new IllegalStateException(
          String.format("Requested OAuth implementation for %s, but it is not included in the oauth mapping.", imageName));
    }
  }

}
