import React from "react";
import { Controller, useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { FormLabel } from "components/forms/FormControl";
import { ListBox, Option } from "components/ui/ListBox";

import { ConnectionScheduleDataBasicSchedule } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { useBasicFrequencyDropdownData } from "./useBasicFrequencyDropdownData";
import { useTrackConnectionFrequency } from "./useTrackConnectionFrequency";
import { FormConnectionFormValues } from "../formConfig";
import { FormFieldLayout } from "../FormFieldLayout";

export const BasicScheduleFormControl: React.FC = () => {
  const { formatMessage } = useIntl();
  const { connection } = useConnectionFormService();
  const { setValue, control } = useFormContext<FormConnectionFormValues>();
  const { trackDropdownSelect } = useTrackConnectionFrequency(connection);
  const frequencies: Array<Option<ConnectionScheduleDataBasicSchedule>> = useBasicFrequencyDropdownData(
    connection.scheduleData
  );

  const onBasicScheduleSelect = (value: ConnectionScheduleDataBasicSchedule): void => {
    trackDropdownSelect(value);
    setValue("scheduleData.basicSchedule", value, { shouldValidate: true, shouldDirty: true });
  };

  return (
    <Controller
      name="scheduleData.basicSchedule"
      control={control}
      render={({ field }) => (
        <FormFieldLayout>
          <FormLabel
            label={formatMessage({ id: "form.frequency" })}
            htmlFor={field.name}
            labelTooltip={formatMessage({
              id: "form.frequency.message",
            })}
          />
          <ListBox<ConnectionScheduleDataBasicSchedule>
            options={frequencies}
            onSelect={onBasicScheduleSelect}
            selectedValue={field.value}
            data-testid="basic-schedule"
          />
        </FormFieldLayout>
      )}
    />
  );
};
