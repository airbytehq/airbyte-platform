import { Icon } from "components/ui/Icon";

import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import styles from "./ThemeToggle.module.scss";

export const ThemeToggle: React.FC = () => {
  const { theme, setTheme } = useAirbyteTheme();

  return (
    <button
      className={styles.button}
      type="button"
      onClick={() => setTheme(theme === "airbyteThemeLight" ? "airbyteThemeDark" : "airbyteThemeLight")}
    >
      {theme === "airbyteThemeLight" ? (
        <Icon type="day" className={styles.icon} />
      ) : (
        <Icon type="moon" className={styles.icon} />
      )}
    </button>
  );
};
