/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airbyte.config.persistence.ConfigRepository;
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
import io.airbyte.oauth.flows.MicrosoftBingAdsOAuthFlow;
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
import io.airbyte.oauth.flows.google.GoogleSearchConsoleOAuthFlow;
import io.airbyte.oauth.flows.google.GoogleSheetsOAuthFlow;
import io.airbyte.oauth.flows.google.YouTubeAnalyticsOAuthFlow;
import java.net.http.HttpClient;
import java.util.Map;

/**
 * OAuth Implementation Factory.
 */
public class OAuthImplementationFactory {

  private final Map<String, OAuthFlowImplementation> oauthFlowMapping;

  public OAuthImplementationFactory(final ConfigRepository configRepository, final HttpClient httpClient) {
    // todo (cgardens) - alphabetize, please.
    final Builder<String, OAuthFlowImplementation> builder = ImmutableMap.builder();
    builder.put("airbyte/source-amazon-ads", new AmazonAdsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-amazon-seller-partner", new AmazonSellerPartnerOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-asana", new AsanaOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-facebook-marketing", new FacebookMarketingOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-facebook-pages", new FacebookPagesOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-github", new GithubOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-google-ads", new GoogleAdsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-google-analytics-v4", new GoogleAnalyticsViewIdOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-google-analytics-data-api", new GoogleAnalyticsPropertyIdOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-google-search-console", new GoogleSearchConsoleOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-google-sheets", new GoogleSheetsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-harvest", new HarvestOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-hubspot", new HubspotOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-intercom", new IntercomOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-instagram", new InstagramOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-lever-hiring", new LeverOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-microsoft-teams", new MicrosoftTeamsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-notion", new NotionOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-bing-ads", new MicrosoftBingAdsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-pinterest", new PinterestOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-pipedrive", new PipeDriveOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-quickbooks", new QuickbooksOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-retently", new RetentlyOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-linkedin-ads", new LinkedinAdsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-salesforce", new SalesforceOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-slack", new SlackOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-smartsheets", new SmartsheetsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-snapchat-marketing", new SnapchatMarketingOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-square", new SquareOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-strava", new StravaOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-surveymonkey", new SurveymonkeyOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-trello", new TrelloOAuthFlow(configRepository));
    builder.put("airbyte/source-gitlab", new GitlabOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-youtube-analytics", new YouTubeAnalyticsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-drift", new DriftOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-xero", new XeroOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-zendesk-chat", new ZendeskChatOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-zendesk-support", new ZendeskSupportOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-zendesk-talk", new ZendeskTalkOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-monday", new MondayOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-zendesk-sunshine", new ZendeskSunshineOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-mailchimp", new MailchimpOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-shopify", new ShopifyOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-tiktok-marketing", new TikTokMarketingOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/destination-snowflake", new DestinationSnowflakeOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/destination-google-sheets", new DestinationGoogleSheetsOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-snowflake", new SourceSnowflakeOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-okta", new OktaOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-paypal-transaction", new PayPalTransactionOAuthFlow(configRepository, httpClient));
    builder.put("airbyte/source-airtable", new AirtableOAuthFlow(configRepository, httpClient)); // revert me
    //
    oauthFlowMapping = builder
        .build();
  }

  /**
   * Returns the OAuthFlowImplementation for a given source or destination, by docker repository.
   *
   * @param imageName - docker repository name for the connector
   * @return OAuthFlowImplementation
   */
  public OAuthFlowImplementation create(final String imageName) {
    if (oauthFlowMapping.containsKey(imageName)) {
      return oauthFlowMapping.get(imageName);
    } else {
      throw new IllegalStateException(
          String.format("Requested OAuth implementation for %s, but it is not included in the oauth mapping.", imageName));
    }
  }

}
