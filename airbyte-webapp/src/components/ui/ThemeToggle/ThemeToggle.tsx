import { useIntl } from "react-intl";

import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";
import { NavItem } from "views/layout/SideBar/components/NavItem";

export const ThemeToggle: React.FC = () => {
  const { theme, setTheme } = useAirbyteTheme();
  const { formatMessage } = useIntl();

  return (
    <NavItem
      as="button"
      label={
        theme === "airbyteThemeLight"
          ? formatMessage({ id: "sidebar.lightMode" })
          : formatMessage({ id: "sidebar.darkMode" })
      }
      icon={theme === "airbyteThemeLight" ? "day" : "moon"}
      onClick={() => setTheme(theme === "airbyteThemeLight" ? "airbyteThemeDark" : "airbyteThemeLight")}
    />
  );
};
