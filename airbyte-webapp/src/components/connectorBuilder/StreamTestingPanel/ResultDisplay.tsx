import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { Paginator } from "components/ui/Paginator";
import { Text } from "components/ui/Text";

import { StreamReadInferredSchema, StreamReadSlicesItem } from "core/api/types/ConnectorBuilderClient";
import { useSelectedPageAndSlice } from "services/connectorBuilder/ConnectorBuilderStateService";

import { PageDisplay } from "./PageDisplay";
import styles from "./ResultDisplay.module.scss";
import { SliceSelector } from "./SliceSelector";

interface ResultDisplayProps {
  slices: StreamReadSlicesItem[];
  inferredSchema?: StreamReadInferredSchema;
  className?: string;
}

export const ResultDisplay: React.FC<ResultDisplayProps> = ({ slices, className, inferredSchema }) => {
  const { selectedSlice, selectedPage, setSelectedSlice, setSelectedPage } = useSelectedPageAndSlice();

  const slice = slices[selectedSlice];
  const numPages = slice.pages.length;
  const page = slice.pages[selectedPage];
  const state = slice.state;

  return (
    <div className={classNames(className, styles.container)}>
      {slices.length > 1 && (
        <SliceSelector
          className={styles.sliceSelector}
          slices={slices}
          selectedSliceIndex={selectedSlice}
          onSelect={setSelectedSlice}
        />
      )}
      <PageDisplay className={styles.pageDisplay} page={page} inferredSchema={inferredSchema} state={state} />
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
