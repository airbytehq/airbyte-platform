import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

import { SupportChatPanel, SupportChatPanelPortal } from "components/ui/support";
import { useAnalyticsTrackFunctions } from "components/ui/support/useAnalyticsTrackFunctions";

import { useCurrentConnectionIdOptional } from "area/connection/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useFeature, FeatureItem } from "core/services/features";

export interface SupportAgentServiceApi {
  openSupportBot: () => void;
  closeSupportBot: () => void;
}

const supportAgentServiceContext = createContext<SupportAgentServiceApi | undefined>(undefined);

export const SupportAgentServiceProvider: React.FC<React.PropsWithChildren> = ({ children }) => {
  const supportEnabled = useFeature(FeatureItem.SupportAgentBot);
  const workspaceId = useCurrentWorkspaceId();
  const connectionId = useCurrentConnectionIdOptional();
  const { trackChatInitiated } = useAnalyticsTrackFunctions();

  const [isOpen, setIsOpen] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [hasBeenOpened, setHasBeenOpened] = useState(false);
  const [conversationKey, setConversationKey] = useState(0);

  useEffect(() => {
    if (hasBeenOpened) {
      trackChatInitiated();
    }
  }, [hasBeenOpened, trackChatInitiated]);

  const openSupportBot = useCallback(() => {
    setHasBeenOpened(true);
    setIsOpen(true);
  }, []);

  const closeSupportBot = useCallback(() => {
    setIsOpen(false);
  }, []);

  const handleNewConversation = useCallback(() => {
    setConversationKey((prev) => prev + 1);
    setHasBeenOpened(true);
    setIsOpen(true);
  }, []);

  const service = useMemo(() => ({ openSupportBot, closeSupportBot }), [openSupportBot, closeSupportBot]);

  return (
    <supportAgentServiceContext.Provider value={service}>
      {children}
      {supportEnabled && hasBeenOpened && (
        <SupportChatPanelPortal isOpen={isOpen}>
          <SupportChatPanel
            key={conversationKey}
            workspaceId={workspaceId}
            connectionId={connectionId}
            isExpanded={isExpanded}
            setIsExpanded={setIsExpanded}
            onClose={closeSupportBot}
            onNewConversation={handleNewConversation}
          />
        </SupportChatPanelPortal>
      )}
    </supportAgentServiceContext.Provider>
  );
};

export const useSupportAgentService = (): SupportAgentServiceApi => {
  const context = useContext(supportAgentServiceContext);
  if (!context) {
    throw new Error("useSupportAgentService must be used within SupportAgentServiceProvider");
  }
  return context;
};
