import { FormConditionItem, FormObjectArrayItem } from "core/form/types";

import { PropertyLabel } from "../Property/PropertyLabel";

interface GroupLabelProps {
  formField: FormConditionItem | FormObjectArrayItem;
}

export const getGroupLabelText = (formField: FormConditionItem | FormObjectArrayItem) =>
  `${formField.title || formField.fieldKey}`;

export const GroupLabel: React.FC<GroupLabelProps> = ({ formField }) => {
  return <PropertyLabel property={formField} label={getGroupLabelText(formField)} optional={false} />;
};
