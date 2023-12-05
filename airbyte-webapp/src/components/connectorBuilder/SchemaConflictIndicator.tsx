import classNames from "classnames";

import { Icon } from "components/ui/Icon";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./SchemaConflictIndicator.module.scss";
import { SchemaConflictMessage } from "./SchemaConflictMessage";

export const SchemaConflictIndicator: React.FC<{ errors?: string[] }> = ({ errors }) => (
  <Tooltip
    control={
      <Icon
        type="warningOutline"
        className={classNames({
          [styles.schemaConflictWarningIcon]: !errors,
          [styles.schemaConflictErrorIcon]: errors,
        })}
      />
    }
  >
    <SchemaConflictMessage errors={errors} />
  </Tooltip>
);
