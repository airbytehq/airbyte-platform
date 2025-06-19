import { useFormContext, useWatch } from "react-hook-form";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";
import { CheckBox } from "components/ui/CheckBox";

import styles from "./ToggleGroupField.module.scss";

interface ToggleGroupFieldProps<T> {
  label: string;
  tooltip?: string;
  fieldPath: string;
  initialValues: T;
}

// eslint-disable-next-line react/function-component-definition
export function ToggleGroupField<T>({
  children,
  label,
  tooltip,
  fieldPath,
  initialValues,
}: React.PropsWithChildren<ToggleGroupFieldProps<T>>) {
  const value = useWatch({ name: fieldPath, exact: false }) as T | undefined;
  const { setValue, clearErrors } = useFormContext();
  const enabled = value !== undefined;

  const labelComponent = (
    <div className={styles.label}>
      <CheckBox
        id={fieldPath}
        checked={enabled}
        onChange={(event) => {
          if (event.target.checked) {
            setValue(fieldPath, initialValues);
          } else {
            setValue(fieldPath, undefined);
            clearErrors(fieldPath);
          }
        }}
      />
      <ControlLabels label={label} infoTooltipContent={tooltip} htmlFor={fieldPath} />
    </div>
  );

  return enabled ? <GroupControls label={labelComponent}>{children}</GroupControls> : labelComponent;
}
