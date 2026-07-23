import React, { createContext, useContext, useEffect, useState } from "react";

import { useWebappConfig } from "core/config";
import { trackError } from "core/utils/datadog";

const FULLSTORY_GUIDES_SCRIPT_ID = "usetifulScript";
const FULLSTORY_GUIDES_API_HOSTNAME = "https://guides.fullstory.com";
const FULLSTORY_GUIDES_SCRIPT_SRC = "https://guides.fullstory.com/dist/gs.js";
const FULLSTORY_GUIDES_LOAD_TIMEOUT = 10_000;

class FullStoryGuidesLoadingError extends Error {}

const FullStoryGuidesContext = createContext(false);

export const useFullStoryGuidesReady = () => useContext(FullStoryGuidesContext);

export const FullStoryGuidesProvider: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { fullstoryGuidesOrgId } = useWebappConfig();
  const [isReady, setIsReady] = useState(false);

  useEffect(() => {
    if (!fullstoryGuidesOrgId) {
      setIsReady(true);
      return;
    }

    if (document.getElementById(FULLSTORY_GUIDES_SCRIPT_ID)) {
      setIsReady(true);
      return;
    }

    const timeout = setTimeout(() => {
      setIsReady((ready) => {
        if (!ready) {
          trackError(new FullStoryGuidesLoadingError("FullStory Guides script timed out"));
        }
        return true;
      });
    }, FULLSTORY_GUIDES_LOAD_TIMEOUT);

    const script = document.createElement("script");
    script.id = FULLSTORY_GUIDES_SCRIPT_ID;
    script.async = true;
    script.src = FULLSTORY_GUIDES_SCRIPT_SRC;
    script.dataset.orgId = fullstoryGuidesOrgId;
    script.dataset.apiHostname = FULLSTORY_GUIDES_API_HOSTNAME;
    script.onload = () => {
      setIsReady(true);
    };
    script.onerror = () => {
      trackError(new FullStoryGuidesLoadingError("FullStory Guides script failed to load"));
      setIsReady(true);
    };

    document.head.appendChild(script);

    return () => clearTimeout(timeout);
  }, [fullstoryGuidesOrgId]);

  return <FullStoryGuidesContext.Provider value={isReady}>{children}</FullStoryGuidesContext.Provider>;
};
