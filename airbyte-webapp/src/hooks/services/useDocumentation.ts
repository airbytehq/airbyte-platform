import { UseQueryResult, useQuery } from "@tanstack/react-query";

import { fetchDocumentation } from "core/domain/Documentation";
import { SupportLevel } from "core/request/AirbyteClient";

import { useAppMonitoringService } from "./AppMonitoringService";
import { useExperiment } from "./Experiment";

type UseDocumentationResult = UseQueryResult<string, Error>;

export const documentationKeys = {
  text: (docsUrl: string) => ["document", docsUrl] as const,
};

export const EMBEDDED_DOCS_PATH = "/docs";

const DOCS_URL = /^https:\/\/docs\.airbyte\.(io|com)/;

const AVAILABLE_INAPP_DOCS = [
  "sources/airtable",
  "sources/amazon-ads",
  "sources/asana",
  "sources/bamboo-hr",
  "sources/bing-ads",
  "sources/exchange-rates",
  "sources/github",
  "sources/google-analytics-v4",
  "sources/google-search-console",
  "sources/google-sheets",
  "sources/instagram",
  "sources/hubspot",
  "sources/jira",
  "sources/notion",
  "sources/salesforce",
  "sources/sendgrid",
  "sources/shopify",
  "sources/slack",
  "sources/zendesk-support",
];

export const useDocumentation = (documentationUrl: string, supportLevel?: SupportLevel): UseDocumentationResult => {
  const shortSetupGuides = useExperiment("connector.shortSetupGuides", false);
  // Get the last two path segments of the documentation URL
  const docName = documentationUrl.substring(
    documentationUrl.lastIndexOf("/", documentationUrl.lastIndexOf("/") - 1) + 1
  );
  const showShortSetupGuide = shortSetupGuides && AVAILABLE_INAPP_DOCS.includes(docName);
  const url = `${documentationUrl.replace(DOCS_URL, EMBEDDED_DOCS_PATH)}${showShortSetupGuide ? ".inapp.md" : ".md"}`;
  const { trackAction } = useAppMonitoringService();

  return useQuery(documentationKeys.text(documentationUrl), () => fetchDocumentation(url, trackAction, supportLevel), {
    enabled: !!documentationUrl,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    retry: false,
  });
};
