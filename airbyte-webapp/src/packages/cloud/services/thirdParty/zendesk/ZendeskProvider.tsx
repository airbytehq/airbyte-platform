import { useEffect } from "react";
import { useEffectOnce } from "react-use";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { config } from "config";

import { useAuthService } from "../../auth/AuthService";

import "./zendesk.module.scss";

declare global {
  interface Window {
    zE?: (type: string, action: string, params?: unknown) => void;
    zESettings?: unknown;
  }
}

export const ZendeskProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { zendeskKey } = config;
  const { user } = useAuthService();
  const workspaceId = useCurrentWorkspaceId();

  useEffectOnce(() => {
    if (zendeskKey) {
      const script = document.createElement("script");
      script.id = "ze-snippet";
      script.src = `https://static.zdassets.com/ekr/snippet.js?key=${zendeskKey}`;
      document.body.appendChild(script);
    }
  });

  useEffect(() => {
    window.zE?.("webWidget", "prefill", {
      name: { value: user?.name },
      email: { value: user?.email },
    });
  }, [user]);

  useEffect(() => {
    window.zESettings = {
      webWidget: {
        contactForm: {
          // Only allow Cloud ticket form
          ticketForms: [{ id: "16332000182427" }],
          fields: [{ id: "16334185233691", prefill: { "*": `https://cloud.airbyte.com/workspaces/${workspaceId}` } }],
        },
      },
    };
  }, [workspaceId]);

  return <>{children}</>;
};
