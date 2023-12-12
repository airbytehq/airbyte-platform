import classNames from "classnames";
import path from "path-browserify";
import React, { useEffect, useMemo, useRef } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation } from "react-router-dom";
import { useUpdateEffect } from "react-use";

import { LoadingPage } from "components";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Markdown } from "components/ui/Markdown";

import {
  GITHUB_DOCS_DESTINATIONS_URL,
  GITHUB_DOCS_SOURCES_URL,
  LOCAL_DOCS_DESTINATIONS_PATH,
  LOCAL_DOCS_SOURCES_PATH,
  REMOTE_DOCS_DESTINATIONS_URL,
  REMOTE_DOCS_SOURCES_URL,
  useConnectorDocumentation,
} from "core/api";
import { isCloudApp } from "core/utils/app";
import { isDevelopment } from "core/utils/isDevelopment";
import { useGetActorIdFromParams } from "core/utils/useGetActorIdFromParams";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import styles from "./DocumentationPanel.module.scss";

const OSS_ENV_MARKERS = /<!-- env:oss -->([\s\S]*?)<!-- \/env:oss -->/gm;
const CLOUD_ENV_MARKERS = /<!-- env:cloud -->([\s\S]*?)<!-- \/env:cloud -->/gm;

export const prepareMarkdown = (markdown: string, env: "oss" | "cloud"): string => {
  // Remove any empty lines between <FieldAnchor> tags and their content, as this causes
  // the content to be rendered as a raw string unless it contains a list, for reasons
  // unknown.
  const preprocessed = markdown.replace(/(<FieldAnchor.*?>)\n{2,}/g, "$1\n");

  return env === "oss" ? preprocessed.replaceAll(CLOUD_ENV_MARKERS, "") : preprocessed.replaceAll(OSS_ENV_MARKERS, "");
};

const ImgRelativePathReplacer: React.FC<
  React.ImgHTMLAttributes<HTMLImageElement> & { actorType?: "source" | "destination" }
> = ({ src, alt, actorType, ...props }) => {
  const isDev = isDevelopment();
  let newSrc: string | undefined;

  if (src === undefined || actorType === undefined) {
    newSrc = src;
  } else if (src.startsWith("../")) {
    if (isDev) {
      newSrc =
        actorType === "source" ? path.join(LOCAL_DOCS_SOURCES_PATH, src) : path.join(LOCAL_DOCS_DESTINATIONS_PATH, src);
    } else {
      const url =
        actorType === "source" ? new URL(src, GITHUB_DOCS_SOURCES_URL) : new URL(src, GITHUB_DOCS_DESTINATIONS_URL);
      newSrc = url.toString();
    }
  } else {
    newSrc = src;
  }

  return <img src={newSrc} alt={alt} {...props} />;
};

const LinkRelativePathReplacer: React.FC<
  React.AnchorHTMLAttributes<HTMLAnchorElement> & { actorType?: "source" | "destination" }
> = ({ href, children, actorType, ...props }) => {
  if (href && href.startsWith("#")) {
    return (
      <a {...props} href={href}>
        {children}
      </a>
    );
  } else if (href && href.startsWith("../")) {
    const docPath = href.replace(/\.md$/, "");
    const url =
      actorType === "source"
        ? new URL(docPath, REMOTE_DOCS_SOURCES_URL)
        : new URL(docPath, REMOTE_DOCS_DESTINATIONS_URL);
    return (
      <a {...props} href={url.toString()} target="_blank" rel="noreferrer">
        {children}
      </a>
    );
  }
  return (
    <a {...props} href={href} target="_blank" rel="noreferrer">
      {children}
    </a>
  );
};

const FieldAnchor: React.FC<React.PropsWithChildren<{ field: string }>> = ({ field, children }) => {
  if (field === "username,password") {
    console.log(children);
  }
  const ref = useRef<HTMLDivElement>(null);
  const { focusedField } = useDocumentationPanelContext();
  const isFieldFocused = field
    .split(",")
    .some((currentField) => focusedField === `connectionConfiguration.${currentField.trim()}`);

  useEffect(() => {
    if (isFieldFocused && ref.current) {
      ref.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [isFieldFocused]);

  return (
    <div ref={ref} className={classNames(styles.focusable, { [styles["focusable--focused"]]: isFieldFocused })}>
      {children}
    </div>
  );
};

export const DocumentationPanel: React.FC = () => {
  const { formatMessage } = useIntl();
  const { setDocumentationPanelOpen, selectedConnectorDefinition } = useDocumentationPanelContext();
  const actorId = useGetActorIdFromParams();
  const { type: actorType, defId: actorDefinitionId } = selectedConnectorDefinition
    ? "sourceDefinitionId" in selectedConnectorDefinition
      ? { type: "source" as const, defId: selectedConnectorDefinition.sourceDefinitionId }
      : { type: "destination" as const, defId: selectedConnectorDefinition.destinationDefinitionId }
    : { type: undefined, defId: undefined };

  const { data, isLoading, error } = useConnectorDocumentation(
    actorType,
    actorDefinitionId,
    actorId,
    selectedConnectorDefinition?.documentationUrl
  );
  const doc = data?.doc;

  const location = useLocation();

  useUpdateEffect(() => {
    setDocumentationPanelOpen(false);
  }, [setDocumentationPanelOpen, location.pathname, location.search]);

  const docsContent = useMemo(
    () =>
      doc && !error
        ? prepareMarkdown(doc, isCloudApp() ? "cloud" : "oss")
        : formatMessage({ id: "connector.setupGuide.notFound" }),
    [doc, error, formatMessage]
  );

  const markdownOptions = useMemo(() => {
    return {
      overrides: {
        img: {
          component: ImgRelativePathReplacer,
          props: {
            actorType,
          },
        },
        a: {
          component: LinkRelativePathReplacer,
          props: {
            actorType,
          },
        },
        FieldAnchor: {
          component: FieldAnchor,
        },
      },
    };
  }, [actorType]);

  return isLoading || !selectedConnectorDefinition?.documentationUrl ? (
    <LoadingPage />
  ) : (
    <FlexContainer className={styles.container} direction="column" gap="none">
      <FlexContainer alignItems="center" className={styles.header} justifyContent="space-between">
        <Heading as="h1">
          <FormattedMessage id="connector.setupGuide" />
        </Heading>
        <ExternalLink href={selectedConnectorDefinition.documentationUrl}>
          <Button variant="secondary" icon={<Icon type="share" />}>
            <FormattedMessage id="connector.setupGuide.fullDocs" />
          </Button>
        </ExternalLink>
      </FlexContainer>
      <Markdown className={styles.content} content={docsContent} options={markdownOptions} />
    </FlexContainer>
  );
};
