import { faMoon } from "@fortawesome/free-regular-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import styles from "./ThemeToggle.module.scss";
import { SunIcon } from "../../icons/SunIcon";

export const ThemeToggle: React.FC = () => {
  const { theme, setTheme } = useAirbyteTheme();

  return (
    <button
      className={styles.button}
      type="button"
      onClick={() => setTheme(theme === "airbyteThemeLight" ? "airbyteThemeDark" : "airbyteThemeLight")}
    >
      {theme === "airbyteThemeLight" ? (
        <SunIcon className={styles.icon} />
      ) : (
        <FontAwesomeIcon className={styles.icon} icon={faMoon} />
      )}
    </button>
  );
};
