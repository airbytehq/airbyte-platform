import { IconType } from "components/ui/Icon";

import { ConnectorIds } from "area/connector/utils";
import { DestinationRead } from "core/api/types/AirbyteClient";

interface SourceCategory {
  id: string;
  labelId: string;
  icon: IconType;
  sourceDefinitionIds: string[];
}

/** The categories and default connectors of Figma frame 5:183893, in its order. */
export const SOURCE_CATEGORIES: SourceCategory[] = [
  {
    id: "marketing",
    labelId: "onboardingFlow.sources.category.marketing",
    icon: "growth",
    sourceDefinitionIds: [
      ConnectorIds.Sources.FacebookMarketing,
      ConnectorIds.Sources.GoogleAds,
      ConnectorIds.Sources.GoogleAnalytics4Ga4,
      ConnectorIds.Sources.TikTokMarketing,
      ConnectorIds.Sources.LinkedInAds,
    ],
  },
  {
    id: "files",
    labelId: "onboardingFlow.sources.category.files",
    icon: "files",
    sourceDefinitionIds: [
      ConnectorIds.Sources.GoogleSheets,
      ConnectorIds.Sources.File,
      ConnectorIds.Sources.S3,
      ConnectorIds.Sources.GoogleDrive,
    ],
  },
  {
    id: "databases",
    labelId: "onboardingFlow.sources.category.databases",
    icon: "database",
    sourceDefinitionIds: [
      ConnectorIds.Sources.Postgres,
      ConnectorIds.Sources.MySql,
      ConnectorIds.Sources.MongoDb,
      ConnectorIds.Sources.MicrosoftSqlServerMssql,
    ],
  },
  {
    id: "ecommerce",
    labelId: "onboardingFlow.sources.category.ecommerce",
    icon: "store",
    sourceDefinitionIds: [
      ConnectorIds.Sources.Shopify,
      ConnectorIds.Sources.Klaviyo,
      ConnectorIds.Sources.AmazonSellerPartner,
      ConnectorIds.Sources.AmazonAds,
    ],
  },
  {
    id: "crm",
    labelId: "onboardingFlow.sources.category.crm",
    icon: "user",
    sourceDefinitionIds: [
      ConnectorIds.Sources.HubSpot,
      ConnectorIds.Sources.Salesforce,
      ConnectorIds.Sources.Pipedrive,
    ],
  },
  {
    id: "payments",
    labelId: "onboardingFlow.sources.category.payments",
    icon: "creditCard",
    sourceDefinitionIds: [ConnectorIds.Sources.Stripe, ConnectorIds.Sources.Chargebee, ConnectorIds.Sources.QuickBooks],
  },
  {
    id: "product",
    labelId: "onboardingFlow.sources.category.product",
    icon: "code",
    sourceDefinitionIds: [
      ConnectorIds.Sources.GitHub,
      ConnectorIds.Sources.Notion,
      ConnectorIds.Sources.Jira,
      ConnectorIds.Sources.Amplitude,
    ],
  },
  {
    id: "support",
    labelId: "onboardingFlow.sources.category.support",
    icon: "comments",
    sourceDefinitionIds: [
      ConnectorIds.Sources.Intercom,
      ConnectorIds.Sources.Slack,
      ConnectorIds.Sources.ZendeskSupport,
    ],
  },
];

/** The destinations of Figma frame 5:184254, in its order. */
export const DESTINATION_OPTIONS = [
  { destinationDefinitionId: ConnectorIds.Destinations.BigQuery, descriptionId: "onboardingFlow.destination.bigQuery" },
  { destinationDefinitionId: ConnectorIds.Destinations.Postgres, descriptionId: "onboardingFlow.destination.postgres" },
  {
    destinationDefinitionId: ConnectorIds.Destinations.Snowflake,
    descriptionId: "onboardingFlow.destination.snowflake",
  },
  {
    destinationDefinitionId: ConnectorIds.Destinations.GoogleSheets,
    descriptionId: "onboardingFlow.destination.googleSheets",
  },
  { destinationDefinitionId: ConnectorIds.Destinations.S3, descriptionId: "onboardingFlow.destination.s3" },
  {
    destinationDefinitionId: ConnectorIds.Destinations.Clickhouse,
    descriptionId: "onboardingFlow.destination.clickhouse",
  },
  {
    destinationDefinitionId: ConnectorIds.Destinations.MotherDuck,
    descriptionId: "onboardingFlow.destination.motherDuck",
  },
  {
    destinationDefinitionId: ConnectorIds.Destinations.DatabricksLakehouse,
    descriptionId: "onboardingFlow.destination.databricks",
  },
];

/** "Not sure yet" recommends Google Sheets, so it creates a Google Sheets draft. */
export const NOT_SURE_DESTINATION = "notSure";
export const RECOMMENDED_DESTINATION_ID = ConnectorIds.Destinations.GoogleSheets;

export const resolveDestinationDefinitionId = (destinationChoice: string) =>
  destinationChoice === NOT_SURE_DESTINATION ? RECOMMENDED_DESTINATION_ID : destinationChoice;

/** A returning member sees the saved draft picked, when it is one of the listed destinations. */
export const getDestinationChoiceFromDrafts = (drafts: DestinationRead[]) =>
  drafts.find(({ destinationDefinitionId }) =>
    DESTINATION_OPTIONS.some((option) => option.destinationDefinitionId === destinationDefinitionId)
  )?.destinationDefinitionId;
