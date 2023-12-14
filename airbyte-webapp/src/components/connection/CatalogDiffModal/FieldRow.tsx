import classnames from "classnames";
import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";
import { Tooltip } from "components/ui/Tooltip";

import { FieldTransform } from "core/api/types/AirbyteClient";

import styles from "./FieldRow.module.scss";

interface FieldRowProps {
  transform: FieldTransform;
}

export const FieldRow: React.FC<FieldRowProps> = ({ transform }) => {
  const fieldName = transform.fieldName.join(".");
  const diffType = transform.transformType.includes("add")
    ? "add"
    : transform.transformType.includes("remove")
    ? "remove"
    : "update";

  const oldType = transform.updateFieldSchema?.oldSchema.type;
  const newType = transform.updateFieldSchema?.newSchema.type;

  const iconStyle = classnames(styles.icon, {
    [styles.plus]: diffType === "add",
    [styles.minus]: diffType === "remove",
    [styles.mod]: diffType === "update",
  });

  const contentStyle = classnames(styles.content, styles.cell, {
    [styles.add]: diffType === "add",
    [styles.remove]: diffType === "remove",
    [styles.update]: diffType === "update",
  });

  const updateCellStyle = classnames(styles.cell, styles.update);
  const hasTypeChange = oldType && newType;

  return (
    <tr
      className={classnames(styles.row, {
        [styles.withType]: hasTypeChange,
      })}
    >
      <td className={contentStyle}>
        <div className={styles.iconContainer}>
          {diffType === "add" ? (
            <Icon type="plus" className={iconStyle} />
          ) : diffType === "remove" ? (
            <Icon type="minus" className={iconStyle} />
          ) : (
            <div className={iconStyle}>
              <Icon type="modification" />
            </div>
          )}
        </div>
        <div title={fieldName} className={styles.field}>
          <div className={styles.fieldName}>{fieldName}</div>
          {transform.breaking && (
            <div className={styles.breakingSchemaChange}>
              <Tooltip
                placement="left"
                control={<Icon type="warningOutline" className={classnames(styles.icon, styles.breakingChange)} />}
              >
                <FormattedMessage id="connection.schemaChange.breaking" />
              </Tooltip>
            </div>
          )}
        </div>
      </td>
      {hasTypeChange && (
        <td className={contentStyle}>
          <div className={updateCellStyle}>
            <span className={styles.dataType}>
              {oldType} <Icon type="arrowRight" /> {newType}
            </span>
          </div>
        </td>
      )}
    </tr>
  );
};
