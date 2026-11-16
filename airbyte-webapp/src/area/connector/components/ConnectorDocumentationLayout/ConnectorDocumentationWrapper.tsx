import { ConnectorDocumentationLayout } from "./ConnectorDocumentationLayout";
import { DocumentationPanelProvider } from "./DocumentationPanelContext";

interface ConnectorDocumentationWrapperProps {
  /** Renders under the form footer slot, at the bottom of the form panel. */
  footer?: React.ReactNode;
}

export const ConnectorDocumentationWrapper: React.FC<React.PropsWithChildren<ConnectorDocumentationWrapperProps>> = ({
  footer,
  children,
}) => {
  return (
    <DocumentationPanelProvider>
      <ConnectorDocumentationLayout footer={footer}>{children}</ConnectorDocumentationLayout>
    </DocumentationPanelProvider>
  );
};
