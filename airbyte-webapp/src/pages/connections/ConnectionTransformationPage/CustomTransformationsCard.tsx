import { FieldArray } from "formik";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

import { ConnectionEditFormCard } from "components/connection/ConnectionEditFormCard";
import { getInitialTransformations } from "components/connection/ConnectionForm/formConfig";
import { TransformationField } from "components/connection/ConnectionForm/TransformationField";
import { DbtOperationReadOrCreate } from "components/connection/TransformationHookForm";

import { OperationCreate, OperationRead } from "core/request/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { FormikOnSubmit } from "types/formik";

// will be used in 2nd part of migration, TransformationFieldHookForm refers to this interface
export interface CustomTransformationsFormValues {
  transformations: DbtOperationReadOrCreate[];
}

export const CustomTransformationsCard: React.FC<{
  operations?: OperationCreate[];
  onSubmit: FormikOnSubmit<{ transformations?: OperationRead[] }>;
}> = ({ operations, onSubmit }) => {
  const [editingTransformation, toggleEditingTransformation] = useToggle(false);
  const { mode } = useConnectionFormService();
  const initialValues = useMemo(
    () => ({
      transformations: getInitialTransformations(operations || []),
    }),
    [operations]
  );

  return (
    <ConnectionEditFormCard<{ transformations?: OperationRead[] }>
      title={<FormattedMessage id="connection.customTransformations" />}
      collapsible
      form={{
        initialValues,
        enableReinitialize: true,
        onSubmit,
      }}
      submitDisabled={editingTransformation}
    >
      <FieldArray name="transformations">
        {(formProps) => (
          <TransformationField
            {...formProps}
            mode={mode}
            onStartEdit={toggleEditingTransformation}
            onEndEdit={toggleEditingTransformation}
          />
        )}
      </FieldArray>
    </ConnectionEditFormCard>
  );
};
