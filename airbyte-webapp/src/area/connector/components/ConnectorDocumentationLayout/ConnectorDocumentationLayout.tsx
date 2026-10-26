import React, { lazy, Suspense, useState } from "react";
import { useIntl } from "react-intl";
import { useWindowSize } from "react-use";

import { LoadingPage } from "components";
import { ResizablePanels } from "components/ui/ResizablePanels";

import { EXCLUDED_DOC_URLS } from "core/api";

import styles from "./ConnectorDocumentationLayout.module.scss";
import { useDocumentationPanelContext } from "./DocumentationPanelContext";
import { FormFooterSlotContext } from "./FormFooterSlotContext";

const LazyDocumentationPanel = lazy(() => import("./DocumentationPanel"));

export const ConnectorDocumentationLayout: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();
  const { documentationPanelOpen, selectedConnectorDefinition } = useDocumentationPanelContext();
  const screenWidth = useWindowSize().width;
  const [formFooterSlot, setFormFooterSlot] = useState<HTMLDivElement | null>(null);
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
          children: (
            <FormFooterSlotContext.Provider value={formFooterSlot}>
              <div className={styles.leftPanel}>
                <div className={styles.leftPanelContent}>{children}</div>
                <div ref={setFormFooterSlot} />
              </div>
            </FormFooterSlotContext.Provider>
          ),
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
