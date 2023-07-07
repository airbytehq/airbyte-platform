import { useCallback, useMemo } from "react";

import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";

export const useZendesk = () => {
  const { trackAction } = useAppMonitoringService();

  const openZendesk = useCallback(() => {
    if (window.zE) {
      window.zE("webWidget", "open");
    } else {
      trackAction(AppActionCodes.ZENDESK_OPEN_FAILURE);
    }
  }, [trackAction]);

  return useMemo(() => ({ openZendesk }), [openZendesk]);
};
