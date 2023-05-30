import * as Flags from "country-flag-icons/react/3x2";
import React from "react";
import { Path } from "react-hook-form";
import { useIntl } from "react-intl";

import { useAvailableGeographies } from "core/api";
import { Geography } from "core/request/AirbyteClient";

import styles from "./DataResidencyDropdown.module.scss";
import { FormValues } from "./Form";
import { FormControl } from "./FormControl";

interface DataResidencyFormControlProps<T extends FormValues> {
  labelId: string;
  name: Path<T>;
  description: React.ReactNode;
  inline?: boolean;
}

export const DataResidencyDropdown = <T extends FormValues>({
  labelId,
  name,
  description,
  inline,
}: DataResidencyFormControlProps<T>): JSX.Element => {
  const { formatMessage } = useIntl();
  const { geographies } = useAvailableGeographies();

  const options = geographies.map((geography) => {
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
  });

  return (
    <FormControl<T>
      label={formatMessage({ id: labelId })}
      description={description}
      fieldType="dropdown"
      name={name}
      options={options}
      inline={inline}
    />
  );
};
