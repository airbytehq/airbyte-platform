import { Field, FieldInputProps, FieldProps, FormikProps, useField } from "formik";
import React, { ChangeEvent, useCallback, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ControlLabels } from "components";
import { Box } from "components/ui/Box";
import { DropDown, DropDownOptionDataItem } from "components/ui/DropDown";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { ConnectionScheduleData, ConnectionScheduleType } from "core/request/AirbyteClient";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { isCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import availableCronTimeZones from "./availableCronTimeZones.json";
import { FormikConnectionFormValues, useFrequencyDropdownData } from "./formConfig";
import { FormFieldLayout } from "./FormFieldLayout";
import styles from "./ScheduleField.module.scss";

const CRON_DEFAULT_VALUE = {
  cronTimeZone: "UTC",
  // Fire at 12:00 PM (noon) every day
  cronExpression: "0 0 12 * * ?",
};

const CronErrorChatWithUsButton: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  return <ExternalLink href={links.supportPortal}>{children}</ExternalLink>;
};

export const ScheduleField: React.FC = () => {
  const { formatMessage } = useIntl();
  const { connection, mode } = useConnectionFormService();
  const frequencies = useFrequencyDropdownData(connection.scheduleData);
  const analyticsService = useAnalyticsService();

  const onDropDownSelect = useCallback(
    (item: DropDownOptionDataItem | null) => {
      const enabledStreams = connection.syncCatalog.streams.filter((stream) => stream.config?.selected).length;

      if (item) {
        analyticsService.track(Namespace.CONNECTION, Action.FREQUENCY, {
          actionDescription: "Frequency selected",
          frequency: item.label,
          connector_source_definition: connection.source.sourceName,
          connector_source_definition_id: connection.source.sourceDefinitionId,
          connector_destination_definition: connection.destination.destinationName,
          connector_destination_definition_id: connection.destination.destinationDefinitionId,
          available_streams: connection.syncCatalog.streams.length,
          enabled_streams: enabledStreams,
          type: mode,
        });
      }
    },
    [
      analyticsService,
      connection.destination.destinationDefinitionId,
      connection.destination.destinationName,
      connection.source.sourceDefinitionId,
      connection.source.sourceName,
      connection.syncCatalog.streams,
      mode,
    ]
  );

  const onScheduleChange = (item: DropDownOptionDataItem, form: FormikProps<FormikConnectionFormValues>) => {
    onDropDownSelect?.(item);

    let scheduleData: ConnectionScheduleData;
    const isManual = item.value === ConnectionScheduleType.manual;
    const isCron = item.value === ConnectionScheduleType.cron;

    // Set scheduleType for yup validation
    const scheduleType = isManual || isCron ? (item.value as ConnectionScheduleType) : ConnectionScheduleType.basic;

    // Set scheduleData.basicSchedule
    if (isManual || isCron) {
      scheduleData = {
        basicSchedule: undefined,
        cron: isCron ? CRON_DEFAULT_VALUE : undefined,
      };
    } else {
      scheduleData = {
        basicSchedule: item.value,
      };
    }

    form.setValues(
      {
        ...form.values,
        scheduleType,
        scheduleData,
      },
      true
    );
  };

  const getBasicScheduleValue = (value: ConnectionScheduleData, form: FormikProps<FormikConnectionFormValues>) => {
    const { scheduleType } = form.values;

    if (scheduleType === ConnectionScheduleType.basic) {
      return value.basicSchedule;
    }

    if (!scheduleType) {
      return null;
    }

    return formatMessage({
      id: `frequency.${scheduleType}`,
    }).toLowerCase();
  };

  const getZoneValue = (currentSelectedZone = "UTC") => currentSelectedZone;

  const onCronChange = (
    event: DropDownOptionDataItem | ChangeEvent<HTMLInputElement>,
    field: FieldInputProps<ConnectionScheduleData>,
    form: FormikProps<FormikConnectionFormValues>,
    key: string
  ) => {
    form.setFieldValue(field.name, {
      cron: {
        ...field.value?.cron,
        [key]: (event as DropDownOptionDataItem).value
          ? (event as DropDownOptionDataItem).value
          : (event as ChangeEvent<HTMLInputElement>).currentTarget.value,
      },
    });
  };

  const cronTimeZones = useMemo(() => {
    return availableCronTimeZones.map((zone: string) => ({ label: zone, value: zone }));
  }, []);

  const isCron = (form: FormikProps<FormikConnectionFormValues>): boolean => {
    return form.values.scheduleType === ConnectionScheduleType.cron;
  };

  const [, { error: cronValidationError }] = useField("scheduleData.cron.cronExpression");

  return (
    <Field name="scheduleData">
      {({ field, meta, form }: FieldProps<ConnectionScheduleData>) => (
        <>
          <FormFieldLayout>
            <ControlLabels
              className={styles.connectorLabel}
              nextLine
              label={formatMessage({
                id: "form.frequency",
              })}
              infoTooltipContent={formatMessage({
                id: "form.frequency.message",
              })}
            />
            <DropDown
              {...field}
              options={frequencies}
              data-testid="scheduleData"
              onChange={(item) => {
                onScheduleChange(item, form);
              }}
              value={getBasicScheduleValue(field.value, form)}
              isDisabled={form.isSubmitting || mode === "readonly"}
            />
          </FormFieldLayout>
          {isCron(form) && (
            <FormFieldLayout>
              <ControlLabels
                className={styles.connectorLabel}
                nextLine
                error={!!meta.error && meta.touched}
                label={formatMessage({
                  id: "form.cronExpression",
                })}
                infoTooltipContent={formatMessage(
                  {
                    id: "form.cronExpression.message",
                  },
                  {
                    lnk: (lnk: React.ReactNode) => <ExternalLink href={links.cronReferenceLink}>{lnk}</ExternalLink>,
                  }
                )}
              />
              <FlexContainer alignItems="center">
                <Input
                  disabled={form.isSubmitting || mode === "readonly"}
                  error={!!meta.error}
                  data-testid="cronExpression"
                  placeholder={formatMessage({
                    id: "form.cronExpression.placeholder",
                  })}
                  value={field.value?.cron?.cronExpression}
                  onChange={(event: ChangeEvent<HTMLInputElement>) =>
                    onCronChange(event, field, form, "cronExpression")
                  }
                />
                <DropDown
                  className={styles.cronZonesDropdown}
                  options={cronTimeZones}
                  value={getZoneValue(field.value?.cron?.cronTimeZone)}
                  isDisabled={form.isSubmitting || mode === "readonly"}
                  onChange={(item: DropDownOptionDataItem) => onCronChange(item, field, form, "cronTimeZone")}
                />
              </FlexContainer>
              {cronValidationError && (
                <Box mt="sm">
                  <Text color="red" data-testid="cronExpressionError">
                    <FormattedMessage
                      id={cronValidationError}
                      {...(isCloudApp() && cronValidationError === "form.cronExpression.underOneHourNotAllowed"
                        ? {
                            values: {
                              lnk: (btnText: React.ReactNode) => (
                                <CronErrorChatWithUsButton>{btnText}</CronErrorChatWithUsButton>
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
        </>
      )}
    </Field>
  );
};
