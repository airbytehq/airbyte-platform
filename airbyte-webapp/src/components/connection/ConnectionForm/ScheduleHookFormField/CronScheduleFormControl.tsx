import React, { ChangeEvent } from "react";
import { Controller, useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormLabel } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { ExternalLink } from "components/ui/Link";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { ConnectionScheduleDataCron } from "core/request/AirbyteClient";
import { links } from "core/utils/links";

import availableCronTimeZones from "./availableCronTimeZonesHookForm.json";
import styles from "./CronScheduleFormControl.module.scss";
import { FeatureItem, useFeature } from "../../../../core/services/features";
import { FormFieldLayout } from "../FormFieldLayout";
import { HookFormConnectionFormValues } from "../hookFormConfig";

export const CRON_DEFAULT_VALUE: ConnectionScheduleDataCron = {
  cronTimeZone: "UTC",
  // Fire at 12:00 PM (noon) every day
  cronExpression: "0 0 12 * * ?",
};

const cronTimeZones = availableCronTimeZones.map((zone: string) => ({ label: zone, value: zone }));

export const CronScheduleFormControl: React.FC = () => {
  const { formatMessage } = useIntl();
  const { setValue, control } = useFormContext<HookFormConnectionFormValues>();
  const { errors } = useFormState<HookFormConnectionFormValues>();
  const allowSubOneHourCronExpressions = useFeature(FeatureItem.AllowSyncSubOneHourCronExpressions);

  const cronValidationError = errors?.scheduleData?.cron?.cronExpression?.message;

  const onCronExpressionChange = (value: string): void => {
    setValue("scheduleData.cron.cronExpression", value, { shouldValidate: true });
  };
  const onCronTimeZoneSelect = (value: string): void => {
    setValue("scheduleData.cron.cronTimeZone", value, { shouldValidate: true });
  };

  // workaround for case when cursor jumps to the end of the input field on every change
  const watchedScheduleData = useWatch({ name: "scheduleData.cron.cronExpression", control });

  return (
    <Controller
      name="scheduleData.cron"
      control={control}
      render={({ field }) => (
        <FormFieldLayout>
          <FormLabel
            label={formatMessage({ id: "form.cronExpression" })}
            htmlFor={field.name}
            labelTooltip={formatMessage(
              {
                id: "form.cronExpression.message",
              },
              {
                lnk: (lnk: React.ReactNode) => <ExternalLink href={links.cronReferenceLink}>{lnk}</ExternalLink>,
              }
            )}
          />
          <FlexContainer alignItems="flex-start">
            <Input
              placeholder={formatMessage({
                id: "form.cronExpression.placeholder",
              })}
              value={watchedScheduleData}
              onChange={(event: ChangeEvent<HTMLInputElement>) => onCronExpressionChange(event.currentTarget.value)}
              data-testid="cronExpression"
            />
            <ListBox
              options={cronTimeZones}
              adaptiveWidth
              selectedValue={field.value?.cronTimeZone}
              onSelect={onCronTimeZoneSelect}
              optionClassName={styles.cronZoneOption}
              optionsMenuClassName={styles.cronZonesOptionsMenu}
              buttonClassName={styles.cronZonesListBoxBtn}
            />
          </FlexContainer>
          {cronValidationError && (
            <Box mt="sm">
              <Text color="red" data-testid="cronExpressionError">
                <FormattedMessage
                  id={cronValidationError}
                  {...(!allowSubOneHourCronExpressions &&
                  cronValidationError === "form.cronExpression.underOneHourNotAllowed"
                    ? {
                        values: {
                          lnk: (btnText: React.ReactNode) => (
                            <ExternalLink href={links.supportPortal}>{btnText}</ExternalLink>
                          ),
                        },
                      }
                    : {})}
                />
              </Text>
            </Box>
          )}
        </FormFieldLayout>
      )}
    />
  );
};
