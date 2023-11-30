import React, { useMemo } from "react";
import { useFieldArray } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ArrayOfObjectsHookFormEditor } from "components/ArrayOfObjectsEditor";

import { OperationCreate, OperatorType } from "core/api/types/AirbyteClient";
import { isDefined } from "core/utils/common";
import { useModalService } from "hooks/services/Modal";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import { CustomTransformationsFormValues } from "pages/connections/ConnectionTransformationPage/CustomTransformationsForm";

import { DbtOperationReadOrCreate, TransformationHookForm } from "../TransformationHookForm";

/**
 * react-hook-form custom transformations form
 */
export const TransformationFieldHookForm: React.FC = () => {
  const { workspaceId } = useCurrentWorkspace();
  const { fields, append, remove, update } = useFieldArray<CustomTransformationsFormValues>({
    name: "transformations",
  });
  const { openModal, closeModal } = useModalService();

  const defaultTransformation: OperationCreate = useMemo(
    () => ({
      name: "My dbt transformations",
      workspaceId,
      operatorConfiguration: {
        operatorType: OperatorType.dbt,
        dbt: {
          gitRepoUrl: "",
          dockerImage: "fishtownanalytics/dbt:1.0.0",
          dbtArguments: "run",
        },
      },
    }),
    [workspaceId]
  );

  const openEditModal = (transformationItemIndex?: number) =>
    openModal({
      size: "xl",
      title: <FormattedMessage id={isDefined(transformationItemIndex) ? "form.edit" : "form.add"} />,
      content: () => (
        <TransformationHookForm
          transformation={
            isDefined(transformationItemIndex)
              ? fields[transformationItemIndex]
              : (defaultTransformation as DbtOperationReadOrCreate)
          }
          onDone={(transformation) => {
            isDefined(transformationItemIndex)
              ? update(transformationItemIndex, transformation)
              : append(transformation);
            closeModal();
          }}
          onCancel={closeModal}
        />
      ),
    });

  return (
    <ArrayOfObjectsHookFormEditor
      fields={fields}
      mainTitle={<FormattedMessage id="form.transformationCount" values={{ count: fields.length }} />}
      addButtonText={<FormattedMessage id="form.addTransformation" />}
      renderItemName={(item) => item.name}
      onAddItem={() => openEditModal()}
      onStartEdit={openEditModal}
      onRemove={remove}
    />
  );
};
