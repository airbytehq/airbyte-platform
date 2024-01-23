import React from "react";
import { useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { Card } from "components/ui/Card";

import { CardFormFieldLayout } from "../ConnectionForm/CardFormFieldLayout";

export const ConnectionNameCard: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <Card title={formatMessage({ id: "connection.title" })}>
      <CardFormFieldLayout>
        <FormControl
          name="name"
          fieldType="input"
          label={formatMessage({ id: "form.connectionName" })}
          placeholder={formatMessage({ id: "form.connectionName.placeholder" })}
          labelTooltip={formatMessage({ id: "form.connectionName.message" })}
          data-testid="connectionName"
          inline
        />
      </CardFormFieldLayout>
    </Card>
  );
};
