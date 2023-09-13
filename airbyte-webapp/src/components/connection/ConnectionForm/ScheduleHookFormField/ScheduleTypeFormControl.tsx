import React from "react";
import { Controller, useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { FormLabel } from "components/forms/FormControl";
import { ListBox, Option } from "components/ui/ListBox";

import { ConnectionScheduleType } from "core/request/AirbyteClient";

import { CRON_DEFAULT_VALUE } from "./CronScheduleFormControl";
import { BASIC_FREQUENCY_DEFAULT_VALUE } from "./useBasicFrequencyDropdownDataHookForm";
import { FormFieldLayout } from "../FormFieldLayout";
import { HookFormConnectionFormValues } from "../hookFormConfig";

export const ScheduleTypeFormControl: React.FC = () => {
  const { formatMessage } = useIntl();
  const { setValue, control } = useFormContext<HookFormConnectionFormValues>();

  const scheduleTypeOptions: Array<Option<ConnectionScheduleType>> = [
    {
      label: formatMessage({
        id: "frequency.scheduled",
      }),
      value: ConnectionScheduleType.basic,
    },
    {
      label: formatMessage({
        id: "frequency.manual",
      }),
      value: ConnectionScheduleType.manual,
    },
    {
      label: formatMessage({
        id: "frequency.cron",
      }),
      value: ConnectionScheduleType.cron,
    },
  ];

  const onScheduleTypeSelect = (value: ConnectionScheduleType): void => {
    // reset scheduleData since we don't need it for manual
    if (value === ConnectionScheduleType.manual) {
      setValue("scheduleData", undefined, { shouldValidate: true });
      return;
    }
    // set default basic schedule
    if (value === ConnectionScheduleType.basic) {
      setValue("scheduleData", { basicSchedule: BASIC_FREQUENCY_DEFAULT_VALUE }, { shouldValidate: true });
      return;
    }
    // set default cron schedule
    if (value === ConnectionScheduleType.cron) {
      setValue("scheduleData", { cron: CRON_DEFAULT_VALUE }, { shouldValidate: true });
    }
  };

  return (
    <Controller
      name="scheduleType"
      control={control}
      render={({ field }) => (
        <FormFieldLayout>
          <FormLabel
            label={formatMessage({ id: "form.scheduleType" })}
            htmlFor={field.name}
            labelTooltip={formatMessage({
              id: "form.scheduleType.message",
            })}
          />
          <ListBox<ConnectionScheduleType>
            options={scheduleTypeOptions}
            onSelect={(value) => {
              field.onChange(value);
              onScheduleTypeSelect(value);
            }}
            selectedValue={field.value}
          />
        </FormFieldLayout>
      )}
    />
  );
};
