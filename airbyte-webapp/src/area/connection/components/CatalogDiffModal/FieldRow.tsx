import classnames from "classnames";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { FieldName, FieldTransform, StreamAttributeTransform } from "core/api/types/AirbyteClient";
import { assertNever } from "core/utils/asserts";

import styles from "./FieldRow.module.scss";

interface FieldRowProps {
  transform: FieldTransform | StreamAttributeTransform;
}

const formatFieldType = (type: unknown): string | undefined => {
  if (typeof type === "string" && !Array.isArray(type)) {
    return type;
  } else if (Array.isArray(type)) {
    return type.join(", ");
  }
  return undefined;
};

const formatFieldName = (fieldName: FieldName | undefined): string => {
  return fieldName?.join(".") ?? "-";
};

const formatFieldNames = (fieldNames: FieldName[] | undefined): string => {
  if (!fieldNames?.length) {
    return "";
  } else if (fieldNames.length === 1) {
    return formatFieldName(fieldNames[0]);
  }
  return `[${fieldNames?.map((f) => formatFieldName(f)).join(", ")}]`;
};

const useFieldNameCell = (
  transform: FieldTransform | StreamAttributeTransform
): { formatted: React.ReactNode; label: string } => {
  const { formatMessage } = useIntl();
  switch (transform.transformType) {
    case "update_primary_key":
      const oldPk = formatFieldNames(transform.updatePrimaryKey?.oldPrimaryKey);
      const newPk = formatFieldNames(transform.updatePrimaryKey?.newPrimaryKey);
      return {
        formatted: (
          <FlexContainer gap="none" alignItems="center">
            <Text color={oldPk ? undefined : "grey"}>
              {oldPk ? oldPk : <FormattedMessage id="connection.schemaChange.noPrimaryKey" />}
            </Text>
            <Icon type="arrowRight" />
            <Text color={newPk ? undefined : "grey"}>
              {newPk ? newPk : <FormattedMessage id="connection.schemaChange.noPrimaryKey" />}
            </Text>
          </FlexContainer>
        ),
        label: formatMessage(
          { id: "connection.schemaChange.primaryKeyChangeLabel" },
          {
            old: oldPk || formatMessage({ id: "connection.schemaChange.noPrimaryKey" }),
            new: newPk || formatMessage({ id: "connection.schemaChange.noPrimaryKey" }),
          }
        ),
      };
    case "add_field":
    case "remove_field":
    case "update_field_schema":
      return {
        formatted: formatFieldName(transform.fieldName),
        label: formatFieldName(transform.fieldName),
      };
    default:
      assertNever(transform);
  }
};

export const FieldRow: React.FC<FieldRowProps> = ({ transform }) => {
  const fieldName = useFieldNameCell(transform);
  const diffType = transform.transformType.includes("add")
    ? "add"
    : transform.transformType.includes("remove")
    ? "remove"
    : "update";

  const oldType =
    "updateFieldSchema" in transform ? formatFieldType(transform.updateFieldSchema?.oldSchema.type) : undefined;
  const newType =
    "updateFieldSchema" in transform ? formatFieldType(transform.updateFieldSchema?.newSchema.type) : undefined;

  const iconStyle = classnames(styles.icon, {
    [styles.plus]: diffType === "add",
    [styles.minus]: diffType === "remove",
    [styles.mod]: diffType === "update",
  });

  const contentStyle = classnames(styles.content, styles.cell, {
    [styles.add]: diffType === "add",
    [styles.remove]: diffType === "remove",
    [styles.update]: diffType === "update",
    [styles.attributeChange]: transform.transformType === "update_primary_key",
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
        <div title={fieldName.label} className={styles.field} data-testid="fieldRow">
          <div className={styles.fieldName}>{fieldName.formatted}</div>
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
