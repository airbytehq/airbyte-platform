import { useCallback, useMemo } from "react";

import { AppActionCodes, trackAction } from "core/utils/datadog";

export const useZendesk = () => {
  const openZendesk = useCallback(() => {
    if (window.zE) {
      window.zE("webWidget", "open");
    } else {
      trackAction(AppActionCodes.ZENDESK_OPEN_FAILURE);
    }
  }, []);

  return useMemo(() => ({ openZendesk }), [openZendesk]);
};
