import React, { lazy, Suspense } from "react";
import { useIntl } from "react-intl";
import { useWindowSize } from "react-use";

import { LoadingPage } from "components/LoadingPage";
import { ResizablePanels } from "components/ui/ResizablePanels";

import { EXCLUDED_DOC_URLS } from "core/api";

import styles from "./ConnectorDocumentationLayout.module.scss";
import { useDocumentationPanelContext } from "./DocumentationPanelContext";

const LazyDocumentationPanel = lazy(() =>
  import("./DocumentationPanel").then(({ DocumentationPanel }) => ({ default: DocumentationPanel }))
);

export const ConnectorDocumentationLayout: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();
  const { documentationPanelOpen, selectedConnectorDefinition } = useDocumentationPanelContext();
  const screenWidth = useWindowSize().width;
  const showDocumentationPanel =
    screenWidth > 500 &&
    documentationPanelOpen &&
    selectedConnectorDefinition?.documentationUrl &&
    !EXCLUDED_DOC_URLS.includes(selectedConnectorDefinition.documentationUrl);

  const documentationPanel = (
    <Suspense fallback={<LoadingPage />}>
      <LazyDocumentationPanel />
    </Suspense>
  );

  return (
    <ResizablePanels
      onlyShowFirstPanel={!showDocumentationPanel}
      panels={[
        {
          children,
          className: styles.leftPanel,
          minWidth: 500,
        },
        {
          children: documentationPanel,
          className: styles.rightPanel,
          minWidth: 60,
          overlay: {
            displayThreshold: 350,
            header: formatMessage({ id: "connector.setupGuide" }),
            rotation: "counter-clockwise",
          },
        },
      ]}
    />
  );
};
