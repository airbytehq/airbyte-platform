import classNames from "classnames";

import { Text } from "components/ui/Text";

import styles from "./InitialBadge.module.scss";

interface InitialBadgeProps {
  inputString: string;
  hashingString: string;
}
type ColorOptions =
  | "green900"
  | "green600"
  | "green300"
  | "darkBlue200"
  | "blue900"
  | "orange900"
  | "orange300"
  | "blue200"
  | "blue400"
  | "darkBlue400";

export const InitialBadge: React.FC<InitialBadgeProps> = ({ inputString, hashingString: additionalString }) => {
  const initials = inputString
    .split(" ")
    .filter(Boolean)
    .map((word) => word[0].toLocaleUpperCase())
    .slice(0, 2)
    .join("\u200b"); // zero-width character to prevent characters from forming ligatures in certain scripts

  const colorOptions: ColorOptions[] = [
    "green900",
    "green600",
    "green300",
    "darkBlue200",
    "blue900",
    "orange900",
    "orange300",
    "blue200",
    "blue400",
    "darkBlue400",
  ];
  const colorStyles = {
    green900: styles.green900,
    green600: styles.green600,
    green300: styles.green300,
    darkBlue200: styles.darkBlue200,
    blue900: styles.blue900,
    orange900: styles.orange900,
    orange300: styles.orange300,
    blue200: styles.blue200,
    blue400: styles.blue400,
    darkBlue400: styles.darkBlue400,
  };

  const randomColor =
    colorOptions[
      (additionalString ?? "").split("").reduce((sum, char) => sum + char.charCodeAt(0), 0) % colorOptions.length
    ];

  return (
    <span className={classNames(styles.initialBadge__container, colorStyles[randomColor])}>
      <Text size="xs" inverseColor>
        {initials}
      </Text>
    </span>
  );
};
