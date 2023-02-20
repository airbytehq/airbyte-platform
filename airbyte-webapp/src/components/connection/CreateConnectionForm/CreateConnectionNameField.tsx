import { Field, FieldProps } from "formik";
import { FormattedMessage, useIntl } from "react-intl";

import { Section } from "components/connection/ConnectionForm/Section";
import { ControlLabels } from "components/LabeledControl";
import { Input } from "components/ui/Input";

import { FormFieldWrapper } from "../ConnectionForm/FormFieldWrapper";

export const CreateConnectionNameField = () => {
  const { formatMessage } = useIntl();

  return (
    <Section title={<FormattedMessage id="connection.title" />}>
      <Field name="name">
        {({ field, meta, form }: FieldProps<string>) => (
          <FormFieldWrapper>
            <ControlLabels
              nextLine
              error={!!meta.error && meta.touched}
              label={<FormattedMessage id="form.connectionName" />}
              infoTooltipContent={formatMessage({
                id: "form.connectionName.message",
              })}
            />
            <Input
              {...field}
              error={!!meta.error}
              data-testid="connectionName"
              disabled={form.isSubmitting}
              placeholder={formatMessage({
                id: "form.connectionName.placeholder",
              })}
            />
          </FormFieldWrapper>
        )}
      </Field>
    </Section>
  );
};
