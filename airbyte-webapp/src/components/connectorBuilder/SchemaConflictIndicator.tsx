import { faWarning } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";

import { Tooltip } from "components/ui/Tooltip";

import styles from "./SchemaConflictIndicator.module.scss";
import { SchemaConflictMessage } from "./SchemaConflictMessage";

export const SchemaConflictIndicator: React.FC<{ errors?: string[] }> = ({ errors }) => (
  <Tooltip
    control={
      <FontAwesomeIcon
        icon={faWarning}
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
