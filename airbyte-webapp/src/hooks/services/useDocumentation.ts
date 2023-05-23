import { UseQueryResult, useQuery } from "react-query";

import { fetchDocumentation } from "core/domain/Documentation";

import { useExperiment } from "./Experiment";

type UseDocumentationResult = UseQueryResult<string, Error>;

export const documentationKeys = {
  text: (docsUrl: string) => ["document", docsUrl] as const,
};

export const EMBEDDED_DOCS_PATH = "/docs";

const DOCS_URL = /^https:\/\/docs\.airbyte\.(io|com)/;

const AVAILABLE_INAPP_DOCS = ["hubspot", "google-analytics-v4", "notion", "google-search-console", "instagram"];

export const useDocumentation = (documentationUrl: string): UseDocumentationResult => {
  const shortSetupGuides = useExperiment("connector.shortSetupGuides", false);
  const docName = documentationUrl.substring(documentationUrl.lastIndexOf("/") + 1);
  const showShortSetupGuide = shortSetupGuides && AVAILABLE_INAPP_DOCS.includes(docName);
  const url = `${documentationUrl.replace(DOCS_URL, EMBEDDED_DOCS_PATH)}${showShortSetupGuide ? ".inapp.md" : ".md"}`;

  return useQuery(documentationKeys.text(documentationUrl), () => fetchDocumentation(url), {
    enabled: !!documentationUrl,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    retry: false,
  });
};
