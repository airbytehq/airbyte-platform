import React, { useLayoutEffect, useState } from "react";

import { useLocalStorage } from "core/utils/useLocalStorage";

import styles from "./useAirbyteTheme.module.scss";

export type Theme = "airbyteThemeLight" | "airbyteThemeDark";

interface AirbyteTheme {
  theme: Theme;
  colorValues: Record<string, string>;
  setTheme: (theme: Theme) => void;
}

export const AirbyteThemeContext = React.createContext<AirbyteTheme | null>(null);

interface AirbyteThemeProviderProps {
  themeOverride?: Theme;
}

const extractVariableNameFromCssVar = /^var\((.*)\)$/;

const themeProxyHandler: ProxyHandler<Record<string, string>> = {
  get(target, prop: string) {
    if (prop in target) {
      return target[prop];
    }
    console.error(`attempted to resolve unknown color variable ${prop}`);
    return "hsl(0, 0%, 0%)";
  },
};

export const AirbyteThemeProvider: React.FC<React.PropsWithChildren<AirbyteThemeProviderProps>> = ({
  children,
  themeOverride,
}) => {
  const [storedTheme, setStoredTheme] = useLocalStorage(
    "airbyteTheme",
    getPreferredColorScheme() === "dark" ? "airbyteThemeDark" : "airbyteThemeLight"
  );

  const theme = themeOverride ? themeOverride : storedTheme;

  const [colorValues, setColorValues] = useState<Record<string, string>>({});

  // useLayoutEffect needed here so that these css classes are modified before the CodeEditor is re-rendered,
  // as that component needs to have the right colors set before rendering to have them properly shown
  useLayoutEffect(() => {
    document.body.classList.remove("airbyteThemeLight");
    document.body.classList.remove("airbyteThemeDark");
    document.body.classList.add(theme);

    const colorValues: Record<string, string> = {};
    const colorVariables = styles.colorVariables.split(" ");
    const bodyStyle = window.getComputedStyle(document.body);
    for (let i = 0; i < colorVariables.length; i++) {
      const colorVariable = colorVariables[i];
      const variableName = colorVariable.replace(extractVariableNameFromCssVar, "$1");
      colorValues[colorVariable] = bodyStyle.getPropertyValue(variableName).trim();
    }

    setColorValues(new Proxy(colorValues, themeProxyHandler));
  }, [theme]);

  const ctx = {
    theme,
    colorValues,
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
