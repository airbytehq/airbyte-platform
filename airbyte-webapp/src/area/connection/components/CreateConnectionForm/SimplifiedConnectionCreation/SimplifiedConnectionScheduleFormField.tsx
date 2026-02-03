import { Listbox } from "@headlessui/react";
import isEqual from "lodash/isEqual";
import uniqueId from "lodash/uniqueId";
import { ChangeEvent, useState } from "react";
import { Controller, useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { FlexContainer } from "components/ui/Flex";
import { FormControlFooter, FormControlFooterInfo, FormControlErrorMessage } from "components/ui/forms/FormControl";
import { Input } from "components/ui/Input";
import { ControlLabels } from "components/ui/LabeledControl";
import { ExternalLink } from "components/ui/Link";
import { ListBox, Option } from "components/ui/ListBox";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

import { FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { FormFieldLayout } from "area/connection/components/ConnectionForm/FormFieldLayout";
import {
  BASIC_FREQUENCY_DEFAULT_VALUE,
  useBasicFrequencyDropdownData,
} from "area/connection/components/ConnectionForm/ScheduleFormField/useBasicFrequencyDropdownData";
import { useTrackConnectionFrequency } from "area/connection/components/ConnectionForm/ScheduleFormField/useTrackConnectionFrequency";
import { useConnectionFormService } from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useDescribeCronExpression } from "core/api";
import { ConnectionScheduleDataBasicSchedule, ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { cronTimeZones, CRON_DEFAULT_VALUE } from "core/utils/cron";
import { links } from "core/utils/links";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";

import { InputContainer } from "./InputContainer";
import styles from "./SimplifiedConnectionScheduleFormField.module.scss";

export const I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED = "form.cronExpression.underOneHourNotAllowed";

export const SimplifiedConnectionScheduleFormField: React.FC<{ disabled: boolean }> = ({ disabled }) => {
  const watchedScheduleType = useWatch<FormConnectionFormValues>({ name: "scheduleType" });

  return (
    <>
      <SimplifiedScheduleTypeFormControl disabled={disabled} />
      {watchedScheduleType === ConnectionScheduleType.basic && (
        <SimplifiedBasicScheduleFormControl disabled={disabled} />
      )}
      {watchedScheduleType === ConnectionScheduleType.cron && <SimplifiedCronScheduleFormControl disabled={disabled} />}
    </>
  );
};

const SimplifiedScheduleTypeFormControl: React.FC<{ disabled: boolean }> = ({ disabled }) => {
  const { formatMessage } = useIntl();
  const { setValue, control } = useFormContext<FormConnectionFormValues>();
  const { defaultValues } = useFormState<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);

  const scheduleTypeOptions: Array<Option<ConnectionScheduleType>> = [
    {
      label: formatMessage({
        id: "frequency.basic",
      }),
      value: ConnectionScheduleType.basic,
      "data-testid": "scheduled",
    },
    {
      label: formatMessage({
        id: "frequency.manual",
      }),
      value: ConnectionScheduleType.manual,
      "data-testid": "manual",
    },
    {
      label: formatMessage({
        id: "frequency.cron",
      }),
      value: ConnectionScheduleType.cron,
      "data-testid": "cron",
    },
  ];

  const onScheduleTypeSelect = (value: ConnectionScheduleType): void => {
    setValue("scheduleType", value, { shouldValidate: true });

    // reset scheduleData since we don't need it for manual
    if (value === ConnectionScheduleType.manual) {
      setValue("scheduleData", undefined, { shouldValidate: true, shouldDirty: true });
      return;
    }
    // set default basic schedule
    if (value === ConnectionScheduleType.basic) {
      setValue(
        "scheduleData",
        // @ts-expect-error react-hook-form makes every value in defaultValues optional
        // which doesn't match our types or usage
        { basicSchedule: defaultValues?.scheduleData?.basicSchedule ?? BASIC_FREQUENCY_DEFAULT_VALUE },
        { shouldValidate: true, shouldDirty: true }
      );
      return;
    }
    // set default cron schedule
    if (value === ConnectionScheduleType.cron) {
      setValue(
        "scheduleData",
        // @ts-expect-error react-hook-form makes every value in defaultValues optional
        // which doesn't match our types or usage
        { cron: defaultValues?.scheduleData?.cron ?? CRON_DEFAULT_VALUE },
        { shouldValidate: true, shouldDirty: true }
      );
    }
  };

  return (
    <Controller
      name="scheduleType"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="form.scheduleType" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage id="form.scheduleType.subtitle" />
                </Text>
              </FlexContainer>
            }
          />
          <InputContainer highlightAfterRedirect>
            <ListBox<ConnectionScheduleType>
              isDisabled={disabled}
              id={controlId}
              options={scheduleTypeOptions}
              onSelect={(value) => {
                onScheduleTypeSelect(value);
              }}
              selectedValue={field.value}
              data-testid="schedule-type"
            />
          </InputContainer>
        </FormFieldLayout>
      )}
    />
  );
};

const SimplifiedBasicScheduleFormControl: React.FC<{ disabled: boolean }> = ({ disabled }) => {
  const { connection } = useConnectionFormService();
  const { setValue, control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const { trackDropdownSelect } = useTrackConnectionFrequency(connection);
  const { isUnifiedTrialPlan } = useOrganizationSubscriptionStatus();
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
      render={({ field }) => {
        const selectedOption = frequencies.find((option) => isEqual(option.value, field.value));

        return (
          <FormFieldLayout alignItems="flex-start" nextSizing>
            <ControlLabels
              htmlFor={controlId}
              label={
                <FlexContainer direction="column" gap="sm">
                  <Text bold>
                    <FormattedMessage id="form.frequency" />
                  </Text>
                  <Text size="sm" color="grey">
                    <FormattedMessage id="form.frequency.subtitle" />
                  </Text>
                </FlexContainer>
              }
            />
            <InputContainer>
              <Listbox value={field.value} onChange={onBasicScheduleSelect} disabled={disabled} by={isEqual}>
                <FloatLayout>
                  <ListboxButton
                    id={controlId}
                    data-testid="basic-schedule-listbox-button"
                    onClick={(e: React.MouseEvent<HTMLButtonElement>) => e.stopPropagation()}
                  >
                    {selectedOption ? (
                      <FlexContainer alignItems="center" gap="sm">
                        <Text as="span" size="lg" {...(disabled && { color: "grey300" })}>
                          {selectedOption.label}
                        </Text>
                      </FlexContainer>
                    ) : (
                      <Text as="span" size="lg" color="grey">
                        <FormattedMessage id="form.selectValue" />
                      </Text>
                    )}
                  </ListboxButton>
                  <ListboxOptions adaptiveWidth data-testid="basic-schedule-listbox-options">
                    {frequencies.map((option, index) => (
                      <ListboxOption
                        key={typeof option.label === "string" ? option.label : index}
                        value={option.value}
                        disabled={option.disabled}
                        {...(option["data-testid"] && {
                          "data-testid": `${option["data-testid"]}-option`,
                        })}
                      >
                        <Box p="md">
                          <FlexContainer alignItems="center" justifyContent="space-between">
                            <Text as="span" size="md">
                              {option.label}
                            </Text>
                            {option.value.timeUnit === "minutes" && isUnifiedTrialPlan && (
                              <BrandingBadge product="cloudForTeams" />
                            )}
                          </FlexContainer>
                        </Box>
                      </ListboxOption>
                    ))}
                  </ListboxOptions>
                </FloatLayout>
              </Listbox>
            </InputContainer>
          </FormFieldLayout>
        );
      }}
    />
  );
};

const SimplifiedCronScheduleFormControl: React.FC<{ disabled: boolean }> = ({ disabled }) => {
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const { formatMessage } = useIntl();
  const { setValue, control } = useFormContext<FormConnectionFormValues>();
  const { errors } = useFormState<FormConnectionFormValues>();
  const cronExpression = useWatch({ name: "scheduleData.cron.cronExpression", control });
  const cronExpressionDescription = useDescribeCronExpression(cronExpression);

  const error = errors?.scheduleData?.cron?.cronExpression;

  const onCronExpressionChange = (value: string): void => {
    setValue("scheduleData.cron.cronExpression", value, { shouldValidate: true, shouldDirty: true });
  };
  const onCronTimeZoneSelect = (value: string): void => {
    setValue("scheduleData.cron.cronTimeZone", value, { shouldDirty: true });
  };
  const cronTimeZone = useWatch({ name: "scheduleData.cron.cronTimeZone", control });

  return (
    <Controller
      name="scheduleData.cron"
      control={control}
      render={() => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="form.cronExpression" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage
                    id="form.cronExpression.subtitle"
                    values={{
                      lnk: (children: React.ReactNode) => (
                        <ExternalLink href={links.cronReferenceLink}>{children}</ExternalLink>
                      ),
                    }}
                  />
                </Text>
              </FlexContainer>
            }
          />
          <div className={styles.errorMessageContainer}>
            <FlexContainer alignItems="flex-start">
              <InputContainer>
                <Input
                  disabled={disabled}
                  id={controlId}
                  placeholder={formatMessage({
                    id: "form.cronExpression.placeholder",
                  })}
                  value={cronExpression}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => onCronExpressionChange(event.currentTarget.value)}
                  data-testid="cronExpression"
                />
              </InputContainer>
              <ListBox
                isDisabled={disabled}
                options={cronTimeZones}
                selectedValue={cronTimeZone}
                onSelect={onCronTimeZoneSelect}
                optionClassName={styles.cronZoneOption}
                buttonClassName={styles.cronZonesListBoxBtn}
              />
            </FlexContainer>
            <FormControlFooter>
              {cronExpressionDescription.isFetching && (
                <FormControlFooterInfo>
                  <FormattedMessage id="form.cronExpression.validating" />
                </FormControlFooterInfo>
              )}

              {!cronExpressionDescription.isFetching && (
                <>
                  {error ? (
                    <FormControlErrorMessage<FormConnectionFormValues>
                      name="scheduleData.cron.cronExpression"
                      message={
                        error?.message === I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED ? (
                          <FormattedMessage
                            id={I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED}
                            values={{
                              lnk: (btnText: React.ReactNode) => (
                                <ExternalLink href={links.contactSales}>{btnText}</ExternalLink>
                              ),
                            }}
                          />
                        ) : undefined
                      }
                    />
                  ) : (
                    cronExpressionDescription.data?.isValid && (
                      <FormControlFooterInfo>{cronExpressionDescription.data.cronDescription}</FormControlFooterInfo>
                    )
                  )}
                </>
              )}
            </FormControlFooter>
          </div>
        </FormFieldLayout>
      )}
    />
  );
};
