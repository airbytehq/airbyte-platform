import { useIntl } from "react-intl";

import { NavItem } from "area/layout/SideBar/components/NavItem";
import { useAirbyteTheme } from "core/utils/useAirbyteTheme";

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
