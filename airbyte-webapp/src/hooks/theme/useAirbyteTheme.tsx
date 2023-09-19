import React, { useLayoutEffect } from "react";

import { useLocalStorage } from "core/utils/useLocalStorage";

export type Theme = "airbyteThemeLight" | "airbyteThemeDark";

interface AirbyteTheme {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

export const AirbyteThemeContext = React.createContext<AirbyteTheme | null>(null);

interface AirbyteThemeProviderProps {
  themeOverride?: Theme;
}

export const AirbyteThemeProvider: React.FC<React.PropsWithChildren<AirbyteThemeProviderProps>> = ({
  children,
  themeOverride,
}) => {
  const [storedTheme, setStoredTheme] = useLocalStorage(
    "airbyteTheme",
    getPreferredColorScheme() === "dark" ? "airbyteThemeDark" : "airbyteThemeLight"
  );

  const theme = themeOverride ? themeOverride : storedTheme;

  // useLayoutEffect needed here so that these css classes are modified before the CodeEditor is re-rendered,
  // as that component needs to have the right colors set before rendering to have them properly shown
  useLayoutEffect(() => {
    document.body.classList.remove("airbyteThemeLight");
    document.body.classList.remove("airbyteThemeDark");
    document.body.classList.add(theme);
  }, [theme]);

  const ctx = {
    theme,
    setTheme: setStoredTheme,
  };

  return <AirbyteThemeContext.Provider value={ctx}>{children}</AirbyteThemeContext.Provider>;
};

export const useAirbyteTheme = (): AirbyteTheme => {
  const airbyteTheme = React.useContext(AirbyteThemeContext);
  if (!airbyteTheme) {
    throw new Error("useAirbyteTheme must be used within an AirbyteThemeProvider.");
  }

  return airbyteTheme;
};

const getPreferredColorScheme = () => {
  if (window.matchMedia("(prefers-color-scheme: dark)").matches) {
    return "dark";
  }
  return "light";
};
