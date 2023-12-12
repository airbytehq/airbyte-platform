import * as Flags from "country-flag-icons/react/3x2";
import React, { ReactNode } from "react";
import { Path } from "react-hook-form";
import { useIntl } from "react-intl";

import { useAvailableGeographies } from "core/api";
import { Geography } from "core/api/types/AirbyteClient";

import styles from "./DataResidencyDropdown.module.scss";
import { FormValues } from "./Form";
import { FormControl } from "./FormControl";

interface DataResidencyFormControlProps<T extends FormValues> {
  labelId: string;
  name: Path<T>;
  description?: string | ReactNode;
  labelTooltip?: React.ReactNode;
  inline?: boolean;
  disabled?: boolean;
}

export const DataResidencyDropdown = <T extends FormValues>({
  labelId,
  name,
  description,
  labelTooltip,
  inline,
  disabled = false,
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
      name={name}
      fieldType="dropdown"
      options={options}
      inline={inline}
      label={formatMessage({ id: labelId })}
      description={description}
      labelTooltip={labelTooltip}
      disabled={disabled}
    />
  );
};
