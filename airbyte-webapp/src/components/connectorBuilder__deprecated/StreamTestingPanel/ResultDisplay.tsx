import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { Paginator } from "components/ui/Paginator";
import { Text } from "components/ui/Text";

import { Slice } from "core/api";
import { StreamReadInferredSchema } from "core/api/types/ConnectorBuilderClient";
import { useSelectedPageAndSlice } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import { PageDisplay } from "./PageDisplay";
import styles from "./ResultDisplay.module.scss";
import { SliceSelector } from "./SliceSelector";

interface ResultDisplayProps {
  slices: Slice[];
  inferredSchema?: StreamReadInferredSchema;
  className?: string;
}

export const ResultDisplay: React.FC<ResultDisplayProps> = ({ slices, className, inferredSchema }) => {
  const { selectedSlice, selectedPage, setSelectedSlice, setSelectedPage } = useSelectedPageAndSlice();

  const slice = slices[selectedSlice];
  const numPages = slice.pages.length;
  const page = slice.pages[selectedPage];

  return (
    <div className={classNames(className, styles.container)}>
      {slices.length > 1 && (
        <SliceSelector slices={slices} selectedSliceIndex={selectedSlice} onSelect={setSelectedSlice} />
      )}
      {page && <PageDisplay className={styles.pageDisplay} page={page} inferredSchema={inferredSchema} />}
      {slice.pages.length > 1 && (
        <div className={styles.paginator} data-testid="test-pages">
          <Text className={styles.pageLabel}>
            <FormattedMessage id="connectorBuilder.pageLabel" />
          </Text>
          <Paginator numPages={numPages} onPageChange={setSelectedPage} selectedPage={selectedPage} />
        </div>
      )}
    </div>
  );
};
