import { Field, FieldProps, useFormikContext } from "formik";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { DataGeographyDropdown } from "components/common/DataGeographyDropdown";
import { Section } from "components/connection/ConnectionForm/Section";
import { ControlLabels } from "components/LabeledControl";

import { useAvailableGeographies } from "core/api";
import { Geography } from "core/request/AirbyteClient";
import { links } from "core/utils/links";

import { FormFieldLayout } from "../ConnectionForm/FormFieldLayout";

interface DataResidencyProps {
  name?: string;
}

/**
 * @deprecated it's formik version of DataResidency form control and will be removed in the future, use DataResidencyHookForm instead
 * @see DataResidencyHookFormCard
 */
export const DataResidency: React.FC<DataResidencyProps> = ({ name = "geography" }) => {
  const { formatMessage } = useIntl();
  const { setFieldValue } = useFormikContext();
  const { geographies } = useAvailableGeographies();

  return (
    <Section title={formatMessage({ id: "connection.geographyTitle" })}>
      <Field name={name}>
        {({ field, form }: FieldProps<Geography>) => (
          <FormFieldLayout>
            <ControlLabels
              nextLine
              label={<FormattedMessage id="connection.geographyTitle" />}
              infoTooltipContent={
                <FormattedMessage
                  id="connection.geographyDescription"
                  values={{
                    ipLink: (node: React.ReactNode) => (
                      <a href={links.cloudAllowlistIPsLink} target="_blank" rel="noreferrer">
                        {node}
                      </a>
                    ),
                    docLink: (node: React.ReactNode) => (
                      <a href={links.connectionDataResidency} target="_blank" rel="noreferrer">
                        {node}
                      </a>
                    ),
                  }}
                />
              }
            />
            <DataGeographyDropdown
              isDisabled={form.isSubmitting}
              geographies={geographies}
              value={field.value}
              onChange={(geography) => setFieldValue(name, geography)}
            />
          </FormFieldLayout>
        )}
      </Field>
    </Section>
  );
};
