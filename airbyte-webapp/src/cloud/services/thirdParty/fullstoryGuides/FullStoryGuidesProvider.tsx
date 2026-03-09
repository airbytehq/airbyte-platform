import React, { useEffect } from "react";

import { useWebappConfig } from "core/config";
import { trackError } from "core/utils/datadog";

const FULLSTORY_GUIDES_SCRIPT_ID = "usetifulScript";
const FULLSTORY_GUIDES_API_HOSTNAME = "https://guides.fullstory.com";
const FULLSTORY_GUIDES_SCRIPT_SRC = "https://guides.fullstory.com/dist/gs.js";

class FullStoryGuidesLoadingError extends Error {}

export const FullStoryGuidesProvider: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { fullstoryGuidesOrgId } = useWebappConfig();

  useEffect(() => {
    if (!fullstoryGuidesOrgId || document.getElementById(FULLSTORY_GUIDES_SCRIPT_ID)) {
      return;
    }

    const script = document.createElement("script");
    script.id = FULLSTORY_GUIDES_SCRIPT_ID;
    script.async = true;
    script.src = FULLSTORY_GUIDES_SCRIPT_SRC;
    script.dataset.orgId = fullstoryGuidesOrgId;
    script.dataset.apiHostname = FULLSTORY_GUIDES_API_HOSTNAME;
    script.onerror = () => {
      trackError(new FullStoryGuidesLoadingError("FullStory Guides script failed to load"));
    };

    document.head.appendChild(script);
  }, [fullstoryGuidesOrgId]);

  return <>{children}</>;
};
