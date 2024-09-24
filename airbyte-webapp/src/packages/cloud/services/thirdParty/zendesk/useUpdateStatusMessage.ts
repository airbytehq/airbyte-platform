import { useCallback } from "react";
import { match, P } from "ts-pattern";

import { ACTIVEINCIDENTS_COLORS, COLORS } from "./constants";
import { CurrentStatus, UserEvent } from "./types";
import { createHtmlElement, createMessageElement, createStatusPageLink, getIframeContainer } from "./utils";
import { IMPACT_DISPLAY_MESSAGES, PAGE_STATUS } from "../instatus/constants";
import { findHighestImpactIncident, getCloudStatus, GetCloudStatusResponse } from "../instatus/getCloudStatus";
import { ActiveIncident } from "../instatus/types";

const getCurrenStatus = (summary: GetCloudStatusResponse): CurrentStatus => {
  return match(summary)
    .with({ activeIncidents: P.not(P.nullish) }, ({ activeIncidents }) => {
      const highestImpactIncident = findHighestImpactIncident(activeIncidents) as ActiveIncident;
      return {
        status: PAGE_STATUS.hasIssues,
        impact: highestImpactIncident.impact,
        url: highestImpactIncident.url,
        message: IMPACT_DISPLAY_MESSAGES[highestImpactIncident.impact],
        styles: ACTIVEINCIDENTS_COLORS[highestImpactIncident.impact],
      };
    })
    .with({ activeMaintenances: P.not(P.nullish) }, ({ activeMaintenances }) => {
      return {
        status: PAGE_STATUS.underMaintenance,
        url: activeMaintenances[0].url,
        styles: COLORS.UNDERMAINTENANCE,
        message: "Some systems are under maintenance",
      };
    })
    .otherwise(() => ({
      status: PAGE_STATUS.up,
    }));
};

const addStatusMessageComponent = async ({
  summary,
  action,
}: {
  summary: GetCloudStatusResponse;
  action: UserEvent["action"];
}) => {
  const currentStatus = getCurrenStatus(summary);
  if (currentStatus.status === PAGE_STATUS.up) {
    return;
  }

  const iframe = document.getElementById("webWidget") as HTMLIFrameElement;
  const doc = iframe?.contentDocument;

  const iframeContainer = getIframeContainer(doc, action);
  const header = doc?.querySelector("header");
  const existingStatusElement = doc?.getElementById("cloud-status-message");

  if (iframeContainer && doc && header && !existingStatusElement) {
    const container = createHtmlElement({
      tagName: "div",
      attributes: { id: "cloud-status-message" },
      styles: {
        margin: "auto",
        padding: "10px",
        textAlign: "center",
        alignItems: "center",
        justifyContent: "center",
        width: "100%",
        height: "auto",
        fontSize: "14px",
        background: currentStatus.styles.background,
        color: currentStatus.styles.color,
      },
    });
    const messageContainer = createHtmlElement({
      tagName: "div",
      styles: {
        textAlign: "center",
        color: currentStatus.styles.color,
      },
    });

    messageContainer.appendChild(createMessageElement({ message: currentStatus.message }));
    container.appendChild(messageContainer);
    if (currentStatus.status === PAGE_STATUS.hasIssues) {
      const linkElement = createStatusPageLink({
        url: currentStatus.url,
        color: ACTIVEINCIDENTS_COLORS[currentStatus.impact].color,
      });
      container.appendChild(linkElement);
    }

    iframeContainer.insertBefore(container, header);
  }
};
export const useUpdateStatusMessage = () => {
  const checkAndAddStatusMessage = useCallback(async (action: UserEvent["action"]) => {
    const cloudStatusResponse = await getCloudStatus();
    if (cloudStatusResponse) {
      addStatusMessageComponent({
        summary: cloudStatusResponse,
        action,
      });
    }
  }, []);

  return { checkAndAddStatusMessage };
};
