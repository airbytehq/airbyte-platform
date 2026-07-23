import React, { ReactNode } from "react";
import { Path } from "react-hook-form";
import { useIntl } from "react-intl";

import { useListDataplaneGroups } from "core/api";

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
  const dataplaneGroups = useListDataplaneGroups();

  // NOTE: should disabled dataplanegroups be filtered out?
  const options = dataplaneGroups.map(({ dataplane_group_id, name }) => {
    return {
      label: name,
      value: dataplane_group_id,
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
  const dataplaneGroups = useListDataplaneGroups();

  const options = dataplaneGroups.map(({ dataplane_group_id, name }) => {
    return {
      label: name,
      value: dataplane_group_id,
    };
  });

  return <SelectWrapper name={name} options={options} disabled={disabled} />;
};
