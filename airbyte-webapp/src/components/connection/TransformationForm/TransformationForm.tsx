import React from "react";
import { useIntl } from "react-intl";

import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { useOperationsCheck } from "core/api";

import { dbtOperationReadOrCreateSchema } from "./schema";
import { DbtOperationReadOrCreate } from "./types";

interface TransformationFormProps {
  transformation: DbtOperationReadOrCreate;
  onDone: (tr: DbtOperationReadOrCreate) => void;
  onCancel: () => void;
}

/**
 * react-hook-form Form for create/update transformation
 * @see TransformationForm
 * @param transformation
 * @param onDone
 * @param onCancel
 */
export const TransformationForm: React.FC<TransformationFormProps> = ({ transformation, onDone, onCancel }) => {
  const { formatMessage } = useIntl();
  const operationCheck = useOperationsCheck();

  const onSubmit = async (values: DbtOperationReadOrCreate) => {
    await operationCheck(values);
    onDone(values);
  };

  return (
    <Form<DbtOperationReadOrCreate>
      onSubmit={onSubmit}
      schema={dbtOperationReadOrCreateSchema}
      defaultValues={transformation}
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
        <ModalFormSubmissionButtons submitKey="form.saveTransformation" onCancelClickCallback={onCancel} />
      </ModalFooter>
    </Form>
  );
};
