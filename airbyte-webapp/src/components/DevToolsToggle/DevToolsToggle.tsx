import { useEffect } from "react";

import { useLocalStorage } from "core/utils/useLocalStorage";

declare global {
  interface Window {
    toggleDevTools: () => void;
  }
}

export const DevToolsToggle: React.FC<unknown> = () => {
  const [showDevTools, setShowDevTools] = useLocalStorage("airbyte_show-dev-tools", true);
  if (showDevTools) {
    document.documentElement.style.setProperty("--show-dev-tools", "initial");
  } else {
    document.documentElement.style.setProperty("--show-dev-tools", "none");
  }

  useEffect(() => {
    window.toggleDevTools = () => {
      const newValue = !showDevTools;
      const displayValue = newValue === true ? "initial" : "none";
      document.documentElement.style.setProperty("--show-dev-tools", displayValue);
      setShowDevTools(newValue);
      return newValue;
    };
  }, [setShowDevTools, showDevTools]);

  return null;
};
