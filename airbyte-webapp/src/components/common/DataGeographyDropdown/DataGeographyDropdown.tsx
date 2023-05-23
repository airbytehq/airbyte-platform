import * as Flags from "country-flag-icons/react/3x2";
import { useIntl } from "react-intl";

import { ListBox } from "components/ui/ListBox";

import { Geography } from "core/request/AirbyteClient";

import styles from "./DataGeographyDropdown.module.scss";

interface DataGeographyDropdownProps {
  geographies: Geography[];
  isDisabled?: boolean;
  onChange: (value: Geography) => void;
  value: Geography;
}

export const DataGeographyDropdown: React.FC<DataGeographyDropdownProps> = ({
  geographies,
  isDisabled = false,
  onChange,
  value,
}) => {
  const { formatMessage } = useIntl();

  return (
    <ListBox
      options={geographies.map((geography) => {
        const Flag =
          geography === "auto" ? Flags.US : Flags[geography.toUpperCase() as Uppercase<Exclude<Geography, "auto">>];
        return {
          label: formatMessage({
            id: `connection.geography.${geography}`,
            defaultMessage: geography.toUpperCase(),
          }),
          value: geography,
          icon: <Flag className={styles.flag} />,
        };
      })}
      onSelect={onChange}
      selectedValue={value}
      isDisabled={isDisabled}
    />
  );
};
