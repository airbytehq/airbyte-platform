import { createColumnHelper } from "@tanstack/react-table";
import { useMemo, useState } from "react";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Modal, ModalBody } from "components/ui/Modal";
import { Pre } from "components/ui/Pre";
import { Table } from "components/ui/Table";

import { StreamReadSlicesItemPagesItemRecordsItem } from "core/api/types/ConnectorBuilderClient";

import styles from "./RecordTable.module.scss";

const columnHelper = createColumnHelper<StreamReadSlicesItemPagesItemRecordsItem>();

export const RecordTable = ({ records }: { records: StreamReadSlicesItemPagesItemRecordsItem[] }) => {
  const [modalValue, setModalValue] = useState<{ value: unknown; key: string } | undefined>(undefined);
  const columns = useMemo(() => {
    // calculate columns based on records
    const columnCandidates = new Set<string>();
    records.forEach((record) => {
      Object.keys(record).forEach((key) => columnCandidates.add(key));
    });
    return Array.from(columnCandidates).map((key) =>
      columnHelper.accessor((record) => record[key], {
        id: key,
        cell: (props) => (
          <ExpandableDataCell
            value={props.getValue()}
            selectValue={() => {
              setModalValue({ value: props.getValue(), key });
            }}
          />
        ),
      })
    );
  }, [records]);
  return (
    <>
      <Table className={styles.table} columns={columns} data={records} />
      {modalValue !== undefined && (
        <Modal onClose={() => setModalValue(undefined)} title={modalValue.key}>
          <ModalBody>
            <Pre>{toString(modalValue.value, 2)}</Pre>
          </ModalBody>
        </Modal>
      )}
    </>
  );
};

const ExpandableDataCell = ({ value, selectValue }: { value: unknown; selectValue: () => void }) => {
  const stringRepresentation = useMemo(() => toString(value), [value]);
  return (
    <FlexContainer className={styles.cell}>
      <div className={styles.content}>{stringRepresentation}</div>
      <Button size="xs" variant="clear" className={styles.button} onClick={selectValue}>
        <Icon type="expand" size="sm" />
      </Button>
    </FlexContainer>
  );
};

const toString = (value: unknown, space?: number): string =>
  typeof value === "object" && value ? JSON.stringify(value, null, space) : String(value);
