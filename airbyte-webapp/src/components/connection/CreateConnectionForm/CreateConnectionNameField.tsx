import { Field, FieldProps } from "formik";
import { FormattedMessage, useIntl } from "react-intl";

import { Section } from "components/connection/ConnectionForm/Section";
import { ControlLabels } from "components/LabeledControl";
import { Input } from "components/ui/Input";

import { FormFieldLayout } from "../ConnectionForm/FormFieldLayout";

/**
 * @deprecated it's formik version of CreateConnectionNameField form control and will be removed in the future, use ConnectionNameHookFormCard instead
 * @see ConnectionNameHookFormCard
 * @constructor
 */
export const CreateConnectionNameField = () => {
  const { formatMessage } = useIntl();

  return (
    <Section title={<FormattedMessage id="connection.title" />}>
      <Field name="name">
        {({ field, meta, form }: FieldProps<string>) => (
          <FormFieldLayout>
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
          </FormFieldLayout>
        )}
      </Field>
    </Section>
  );
};
