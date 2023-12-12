import classnames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";
import { Tooltip } from "components/ui/Tooltip";

import { SchemaChange } from "core/api/types/AirbyteClient";
import { convertSnakeToCamel } from "core/utils/strings";

import styles from "./ChangesStatusIcon.module.scss";

interface ChangesStatusIconProps {
  schemaChange?: SchemaChange;
}

export const ChangesStatusIcon: React.FC<ChangesStatusIconProps> = ({ schemaChange = "no_change" }) => {
  if (schemaChange === "no_change") {
    return null;
  }
  const iconStyle = classnames(styles.changesIcon, {
    [styles.breaking]: schemaChange === "breaking",
    [styles.nonBreaking]: schemaChange === "non_breaking",
  });
  return (
    <Tooltip
      placement="left"
      containerClassName={styles.tooltipContainer}
      control={
        <Icon
          className={iconStyle}
          type={schemaChange === "breaking" ? "statusWarning" : "infoFilled"}
          size="lg"
          data-testid={`changesStatusIcon-${schemaChange}`}
        />
      }
    >
      <FormattedMessage id={`connection.schemaChange.${convertSnakeToCamel(schemaChange)}`} />
    </Tooltip>
  );
};
