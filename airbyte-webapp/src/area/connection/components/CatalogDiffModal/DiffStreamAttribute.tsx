import { FormattedMessage } from "react-intl";

import { StreamAttributeTransform } from "core/api/types/AirbyteClient";
import { assertNever } from "core/utils/asserts";

import styles from "./DiffStreamAttribute.module.scss";
import { FieldRow } from "./FieldRow";

interface DiffFieldTableProps {
  transforms: StreamAttributeTransform[];
}

const headerId = (transform: StreamAttributeTransform) => {
  switch (transform.transformType) {
    case "update_primary_key":
      return "connection.updateSchema.primaryKeyChanged";
    default:
      assertNever(transform.transformType);
  }
};

export const DiffStreamAttribute: React.FC<DiffFieldTableProps> = ({ transforms }) => {
  return (
    <>
      {transforms.map((transform) => {
        return (
          <table
            className={styles.diffAttribute}
            key={transform.transformType}
            data-testid={`streamAttributeTable-${transform.transformType}`}
          >
            <thead>
              <tr className={styles.diffAttribute__header}>
                <th>
                  <FormattedMessage id={headerId(transform)} />
                </th>
              </tr>
            </thead>
            <tbody>
              <FieldRow transform={transform} />
            </tbody>
          </table>
        );
      })}
    </>
  );
};
