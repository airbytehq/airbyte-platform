import React, { ReactElement, useMemo } from "react";
import { useFieldArray } from "react-hook-form";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";

import styles from "./BuilderList.module.scss";

interface BuilderListProps {
  children: (props: { buildPath: (path: string) => string }) => ReactElement;
  basePath: string;
  emptyItem: object;
  addButtonLabel: string;
}

export const BuilderList: React.FC<BuilderListProps> = ({ children, emptyItem, basePath, addButtonLabel }) => {
  const { fields, append, remove } = useFieldArray({
    name: basePath,
  });

  const buildPathFunctions = useMemo(
    () =>
      new Array(fields.length).fill(undefined).map((_value, index) => {
        return (path: string) => `${basePath}.${index}${path !== "" ? "." : ""}${path}`;
      }),
    [basePath, fields.length]
  );

  return (
    <>
      {buildPathFunctions.map((buildPath, currentItemIndex) => (
        <div className={styles.itemWrapper} key={fields[currentItemIndex].id}>
          <div className={styles.itemContainer}>{children({ buildPath })}</div>
          <RemoveButton
            onClick={() => {
              remove(currentItemIndex);
            }}
          />
        </div>
      ))}
      <div>
        <Button
          type="button"
          variant="secondary"
          icon={<Icon type="plus" />}
          onClick={() => {
            append({ ...emptyItem });
          }}
        >
          {addButtonLabel}
        </Button>
      </div>
    </>
  );
};
