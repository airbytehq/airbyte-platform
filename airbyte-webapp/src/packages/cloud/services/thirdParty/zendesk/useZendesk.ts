import { useCallback, useMemo } from "react";

import { AppActionCodes, trackAction } from "core/utils/datadog";

export const useZendesk = () => {
  const openZendesk = useCallback(() => {
    let opened = false;
    try {
      if (window.zE) {
        window.zE("webWidget", "open");

        opened = true;
      }
    } catch (e) {}
    if (!opened) {
      trackAction(AppActionCodes.ZENDESK_OPEN_FAILURE);
    }
  }, []);

  return useMemo(() => ({ openZendesk }), [openZendesk]);
};
