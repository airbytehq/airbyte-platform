import { ReleaseStage } from "core/request/AirbyteClient";
import { AppActionCodes, TrackActionFn } from "hooks/services/AppMonitoringService";

export const fetchDocumentation = async (
  url: string,
  trackAction: TrackActionFn,
  releaseStage?: ReleaseStage
): Promise<string> => {
  const response = await fetch(url, {
    method: "GET",
  });

  if (!response.ok) {
    if (releaseStage === ReleaseStage.custom) {
      console.error(`Failed to fetch documentation from ${url} with status ${response.status}`);
    } else {
      trackAction(AppActionCodes.CONNECTOR_DOCUMENTATION_FETCH_ERROR, { url, status: response.status });
    }
  }
  const contentType = response.headers.get("content-type");

  if (contentType?.toLowerCase().includes("text/html")) {
    trackAction(AppActionCodes.CONNECTOR_DOCUMENTATION_NOT_MARKDOWN, { url, contentType });
    throw new Error(`Documentation was text/html and such ignored since markdown couldn't be found.`);
  }

  return await response.text();
};
