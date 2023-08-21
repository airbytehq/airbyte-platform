import React from "react";
import { useFieldArray } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ArrayOfObjectsHookFormEditor } from "components/ArrayOfObjectsEditor";

import { isDefined } from "core/utils/common";
import { useModalService } from "hooks/services/Modal";
import { CustomTransformationsFormValues } from "pages/connections/ConnectionTransformationPage/CustomTransformationsForm";

import { useDefaultTransformation } from "./formConfig";
import { DbtOperationReadOrCreate, TransformationHookForm } from "../TransformationHookForm";

/**
 * Custom transformations field for react-hook-form
 * will replace TransformationField in the future
 * @see TransformationField
 * @constructor
 */
export const TransformationFieldHookForm: React.FC = () => {
  const { fields, append, remove, update } = useFieldArray<CustomTransformationsFormValues>({
    name: "transformations",
  });
  const { openModal, closeModal } = useModalService();
  const defaultTransformation = useDefaultTransformation();

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
