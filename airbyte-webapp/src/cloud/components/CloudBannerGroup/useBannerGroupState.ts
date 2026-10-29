import { useCallback, useMemo, useState } from "react";

import { useCurrentUser } from "core/services/auth";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { BannerGroupContextValue } from "./BannerGroupContext";

const DEFAULT_BANNER_GROUP_STATE = { isCollapsed: false, seenIds: [] as string[] };

// A lone banner is never hidden, since there would be no toggle to show it again.
const MIN_HIDEABLE_BANNERS_TO_COLLAPSE = 2;

interface GroupCollapseInput {
  bannerIds: string[];
  seenIds: string[];
  isPersistedCollapsed: boolean;
}

// The group is collapsed when the user left it collapsed, it has enough banners to need a toggle,
// and the user has seen every one of them.
const checkIsGroupCollapsed = ({ bannerIds, seenIds, isPersistedCollapsed }: GroupCollapseInput) =>
  isPersistedCollapsed &&
  bannerIds.length >= MIN_HIDEABLE_BANNERS_TO_COLLAPSE &&
  bannerIds.every((id) => seenIds.includes(id));

export const useBannerGroupState = () => {
  const { userId } = useCurrentUser();
  const [bannerGroupStateByUserId, setBannerGroupStateByUserId] = useLocalStorage("airbyte_banner-group-state", {});
  const { isCollapsed: isPersistedCollapsed, seenIds } = bannerGroupStateByUserId[userId] ?? DEFAULT_BANNER_GROUP_STATE;
  const [registeredIds, setRegisteredIds] = useState<string[]>([]);

  const hideableCount = registeredIds.length;
  const isToggleShown = hideableCount >= MIN_HIDEABLE_BANNERS_TO_COLLAPSE;
  const isCollapsed = checkIsGroupCollapsed({ bannerIds: registeredIds, seenIds, isPersistedCollapsed });

  const registerBanner = useCallback((id: string) => {
    setRegisteredIds((previous) => (previous.includes(id) ? previous : [...previous, id]));
  }, []);

  const unregisterBanner = useCallback((id: string) => {
    setRegisteredIds((previous) =>
      previous.includes(id) ? previous.filter((registered) => registered !== id) : previous
    );
  }, []);

  // Called during a banner's first render, so evaluate as if the banner were already registered.
  const checkIsBannerHidden = useCallback(
    (id: string) => {
      const bannerIds = registeredIds.includes(id) ? registeredIds : [...registeredIds, id];

      return checkIsGroupCollapsed({ bannerIds, seenIds, isPersistedCollapsed });
    },
    [registeredIds, isPersistedCollapsed, seenIds]
  );

  // Saves what the user chose. Marking every banner on the page as seen is also what stops an unseen banner
  // from forcing a collapsed group open.
  const saveBannerGroupChoice = useCallback(
    (isCollapsed: boolean) => {
      setBannerGroupStateByUserId((previous) => {
        const currentUserState = previous[userId] ?? DEFAULT_BANNER_GROUP_STATE;

        return {
          ...previous,
          [userId]: {
            isCollapsed,
            seenIds: Array.from(new Set([...currentUserState.seenIds, ...registeredIds])),
          },
        };
      });
    },
    [setBannerGroupStateByUserId, userId, registeredIds]
  );

  const collapseBannerGroup = useCallback(() => saveBannerGroupChoice(true), [saveBannerGroupChoice]);

  const expandBannerGroup = useCallback(() => saveBannerGroupChoice(false), [saveBannerGroupChoice]);

  const contextValue = useMemo<BannerGroupContextValue>(
    () => ({ registerBanner, unregisterBanner, checkIsBannerHidden }),
    [registerBanner, unregisterBanner, checkIsBannerHidden]
  );

  return { contextValue, hideableCount, isCollapsed, isToggleShown, collapseBannerGroup, expandBannerGroup };
};
