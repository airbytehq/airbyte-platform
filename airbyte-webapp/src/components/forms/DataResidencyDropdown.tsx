import React, { ReactNode } from "react";
import { Path } from "react-hook-form";
import { useIntl } from "react-intl";

import { useAvailableGeographies } from "core/api";

import { FormValues } from "./Form";
import { FormControl } from "./FormControl";
import { SelectWrapper } from "./SelectWrapper";

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
    return {
      label: formatMessage({
        id: `connection.geography.${geography}`,
        defaultMessage: geography,
      }),
      value: geography,
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

export const StandaloneDataResidencyDropdown = <T extends FormValues>({
  name,
  disabled,
}: Pick<DataResidencyFormControlProps<T>, "name" | "disabled">): JSX.Element => {
  const { formatMessage } = useIntl();
  const { geographies } = useAvailableGeographies();

  const options = geographies.map((geography) => {
    return {
      label: formatMessage({
        id: `connection.geography.${geography}`,
        defaultMessage: geography,
      }),
      value: geography,
    };
  });

  return <SelectWrapper name={name} options={options} disabled={disabled} />;
};
