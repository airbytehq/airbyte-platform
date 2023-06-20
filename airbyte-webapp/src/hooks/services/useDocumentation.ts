import { UseQueryResult, useQuery } from "@tanstack/react-query";

import { fetchDocumentation } from "core/domain/Documentation";

import { useAppMonitoringService } from "./AppMonitoringService";
import { useExperiment } from "./Experiment";

type UseDocumentationResult = UseQueryResult<string, Error>;

export const documentationKeys = {
  text: (docsUrl: string) => ["document", docsUrl] as const,
};

export const EMBEDDED_DOCS_PATH = "/docs";

const DOCS_URL = /^https:\/\/docs\.airbyte\.(io|com)/;

const AVAILABLE_INAPP_DOCS = [
  "airtable",
  "amazon-ads",
  "asana",
  "bamboo-hr",
  "bing-ads",
  "exchange-rates",
  "github",
  "google-analytics-v4",
  "google-search-console",
  "google-sheets",
  "instagram",
  "hubspot",
  "jira",
  "notion",
  "salesforce",
  "sendgrid",
  "shopify",
  "slack",
  "zendesk-support",
];

export const useDocumentation = (documentationUrl: string): UseDocumentationResult => {
  const shortSetupGuides = useExperiment("connector.shortSetupGuides", false);
  const docName = documentationUrl.substring(documentationUrl.lastIndexOf("/") + 1);
  const showShortSetupGuide = shortSetupGuides && AVAILABLE_INAPP_DOCS.includes(docName);
  const url = `${documentationUrl.replace(DOCS_URL, EMBEDDED_DOCS_PATH)}${showShortSetupGuide ? ".inapp.md" : ".md"}`;
  const { trackAction } = useAppMonitoringService();

  return useQuery(documentationKeys.text(documentationUrl), () => fetchDocumentation(url, trackAction), {
    enabled: !!documentationUrl,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    retry: false,
  });
};
