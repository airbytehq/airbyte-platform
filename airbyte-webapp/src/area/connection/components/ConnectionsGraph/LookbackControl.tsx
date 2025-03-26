import classNames from "classnames";
import { useMemo } from "react";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { LookbackWindow } from "./lookbackConfiguration";
import styles from "./LookbackControl.module.scss";

interface LookbackControlProps {
  selected: LookbackWindow;
  setSelected: (value: LookbackWindow) => void;
}

export const LookbackControl: React.FC<LookbackControlProps> = ({ selected, setSelected }) => {
  const { formatMessage } = useIntl();
  const options: Array<{ label: string; value: LookbackWindow }> = useMemo(
    () => [
      { label: formatMessage({ id: "connections.graph.6h" }), value: "6h" },
      { label: formatMessage({ id: "connections.graph.24h" }), value: "24h" },
      { label: formatMessage({ id: "connections.graph.7d" }), value: "7d" },
      { label: formatMessage({ id: "connections.graph.30d" }), value: "30d" },
    ],
    [formatMessage]
  );

  return (
    <fieldset className={styles.lookbackControl}>
      {options.map((option) => (
        <label
          key={option.value}
          className={classNames(styles.lookbackControl__option, {
            [styles["lookbackControl__option--selected"]]: option.value === selected,
          })}
        >
          <input
            type="radio"
            value={option.value}
            checked={selected === option.value}
            onChange={() => setSelected(option.value)}
            className={styles.lookbackControl__input}
          />
          <Text
            color={option.value === selected ? "darkBlue" : "grey"}
            className={styles.lookbackControl__label}
            as="span"
          >
            {option.label}
          </Text>
        </label>
      ))}
    </fieldset>
  );
};
