import { useState, useEffect, useCallback } from "react";

import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";

interface UseSupportChatPanelStateReturn {
  isOpen: boolean;
  setIsOpen: (value: boolean) => void;
  isExpanded: boolean;
  setIsExpanded: (value: boolean) => void;
  hasBeenOpened: boolean;
  setHasBeenOpened: (value: boolean) => void;
  conversationKey: number;
  handleNewConversation: () => void;
}

/**
 * Custom hook for managing Support Chat Panel state.
 *
 * Provides state management for the Support Agent chat panel including:
 * - Panel visibility (open/closed)
 * - Panel size (expanded/compact)
 * - First-open tracking for analytics
 * - Conversation key for remounting chat panel
 *
 * This hook automatically tracks chat initiation analytics when the panel
 * is opened for the first time.
 */
export const useSupportChatPanelState = (): UseSupportChatPanelStateReturn => {
  const { trackChatInitiated } = useAnalyticsTrackFunctions();

  // Widget display state
  const [isOpen, setIsOpen] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [hasBeenOpened, setHasBeenOpened] = useState(false);
  const [conversationKey, setConversationKey] = useState(0);

  // Track chat initiated only once when first opened
  useEffect(() => {
    if (hasBeenOpened) {
      trackChatInitiated();
    }
  }, [hasBeenOpened, trackChatInitiated]);

  const handleNewConversation = useCallback(() => {
    setConversationKey((prev) => prev + 1);
    setHasBeenOpened(true);
    setIsOpen(true);
  }, []);

  return {
    isOpen,
    setIsOpen,
    isExpanded,
    setIsExpanded,
    hasBeenOpened,
    setHasBeenOpened,
    conversationKey,
    handleNewConversation,
  };
};
