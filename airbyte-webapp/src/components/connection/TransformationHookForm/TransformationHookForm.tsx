import React from "react";
import { useIntl } from "react-intl";

import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { useOperationsCheck } from "core/api";
import { useFormChangeTrackerService, useUniqueFormId } from "hooks/services/FormChangeTracker";

import { dbtOperationReadOrCreateSchema } from "./schema";
import { DbtOperationReadOrCreate } from "./types";

interface TransformationHookFormProps {
  transformation: DbtOperationReadOrCreate;
  onDone: (tr: DbtOperationReadOrCreate) => void;
  onCancel: () => void;
}

/**
 * react-hook-form Form for create/update transformation
 * old version of TransformationField
 * @see TransformationForm
 * @param transformation
 * @param onDone
 * @param onCancel
 * @constructor
 */
export const TransformationHookForm: React.FC<TransformationHookFormProps> = ({ transformation, onDone, onCancel }) => {
  const { formatMessage } = useIntl();
  const operationCheck = useOperationsCheck();
  const { clearFormChange } = useFormChangeTrackerService();
  const formId = useUniqueFormId();

  const onSubmit = async (values: DbtOperationReadOrCreate) => {
    await operationCheck(values);
    clearFormChange(formId);
    onDone(values);
  };

  const onFormCancel = () => {
    clearFormChange(formId);
    onCancel();
  };

  return (
    <Form<DbtOperationReadOrCreate>
      onSubmit={onSubmit}
      schema={dbtOperationReadOrCreateSchema}
      defaultValues={transformation}
      // TODO: uncomment when trackDirtyChanges will be fixed. Issue: https://github.com/airbytehq/airbyte/issues/28745
      // trackDirtyChanges
    >
      <ModalBody maxHeight={400}>
        <FlexContainer gap="lg">
          <FlexItem grow>
            <FormControl
              name="name"
              fieldType="input"
              type="text"
              label={formatMessage({ id: "form.transformationName" })}
            />
            <FormControl
              name="operatorConfiguration.dbt.dockerImage"
              fieldType="input"
              type="text"
              label={formatMessage({ id: "form.dockerUrl" })}
            />
            <FormControl
              name="operatorConfiguration.dbt.gitRepoUrl"
              fieldType="input"
              type="text"
              label={formatMessage({ id: "form.repositoryUrl" })}
              placeholder={formatMessage(
                {
                  id: "form.repositoryUrl.placeholder",
                },
                { angle: (node: React.ReactNode) => `<${node}>` }
              )}
            />
          </FlexItem>
          <FlexItem grow>
            <FormControl
              name="operatorConfiguration.dbt.dbtArguments"
              fieldType="input"
              type="text"
              label={formatMessage({ id: "form.entrypoint.linked" })}
            />
            <FormControl
              name="operatorConfiguration.dbt.gitRepoBranch"
              fieldType="input"
              type="text"
              label={formatMessage({ id: "form.gitBranch" })}
            />
          </FlexItem>
        </FlexContainer>
      </ModalBody>
      <ModalFooter>
        <ModalFormSubmissionButtons submitKey="form.saveTransformation" onCancelClickCallback={onFormCancel} />
      </ModalFooter>
    </Form>
  );
};
