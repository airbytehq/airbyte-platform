import { useEffect } from "react";
import { useEffectOnce } from "react-use";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useWebappConfig } from "core/config";
import { useAuthService } from "core/services/auth";

import "./zendesk.scss";
import { ACTIONS } from "./constants";
import { UserEvent } from "./types";
import { useUpdateStatusMessage } from "./useUpdateStatusMessage";

declare global {
  interface Window {
    zE?: (type: string, action: string, params?: unknown) => void;
    zESettings?: unknown;
  }
}

export const ZendeskProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { zendeskKey } = useWebappConfig();
  const { user } = useAuthService();
  const workspaceId = useCurrentWorkspaceId();
  const { checkAndAddStatusMessage } = useUpdateStatusMessage();

  useEffectOnce(() => {
    if (zendeskKey) {
      const script = document.createElement("script");
      script.id = "ze-snippet";
      script.onload = () => {
        if (typeof window.zE === "function") {
          try {
            window.zE("webWidget:on", "userEvent", (userEvent: UserEvent) => {
              if (userEvent.action === ACTIONS.helpCenterShown || userEvent.action === ACTIONS.contactFormShown) {
                console.log("Zendesk userEventAA:", userEvent);
                checkAndAddStatusMessage(userEvent.action);
              }
            });
          } catch (e) {}
        } else {
          console.warn("Zendesk widget not available yet");
        }
      };
      script.src = `https://static.zdassets.com/ekr/snippet.js?key=${zendeskKey}`;
      document.body.appendChild(script);
    }
  });

  useEffect(() => {
    try {
      window.zE?.("webWidget", "prefill", {
        name: { value: user?.name },
        email: { value: user?.email },
      });
    } catch (e) {}
  }, [user]);

  useEffect(() => {
    const config = {
      webWidget: {
        contactForm: {
          // Only allow Cloud ticket form
          ticketForms: [{ id: "16332000182427" }],
          fields: [{ id: "16334185233691", prefill: { "*": `https://cloud.airbyte.com/workspaces/${workspaceId}` } }],
        },
      },
    };
    try {
      // Set settings to be read by ZenDesk when initially loaded
      window.zESettings = config;
      // Update settings in case ZenDesk already has loaded
      window.zE?.("webWidget", "updateSettings", config);
    } catch (e) {}
  }, [workspaceId]);

  return <>{children}</>;
};
