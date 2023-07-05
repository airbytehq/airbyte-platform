import { AppActionCodes, TrackActionFn } from "hooks/services/AppMonitoringService";

export const fetchDocumentation = async (url: string, trackAction: TrackActionFn): Promise<string> => {
  const response = await fetch(url, {
    method: "GET",
  });

  if (!response.ok) {
    trackAction(AppActionCodes.CONNECTOR_DOCUMENTATION_FETCH_ERROR, { url, status: response.status });
  }
  const contentType = response.headers.get("content-type");

  if (contentType?.toLowerCase().includes("text/html")) {
    trackAction(AppActionCodes.CONNECTOR_DOCUMENTATION_NOT_MARKDOWN, { url, contentType });
    throw new Error(`Documentation was text/html and such ignored since markdown couldn't be found.`);
  }

  return await response.text();
};
