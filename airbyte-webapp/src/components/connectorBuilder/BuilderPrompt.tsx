import classNames from "classnames";
import { FormattedMessage } from "react-intl";
import { useResizeDetector } from "react-resize-detector";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import BuilderPromptIcon from "./builder-prompt-icon.svg?react";
import styles from "./BuilderPrompt.module.scss";

interface BuilderPromptProps {
  builderRoutePath: string;
  renderAsButton?: boolean;
  shortDescription?: boolean;
  className?: string;
}

export const BuilderPrompt: React.FC<BuilderPromptProps> = ({
  shortDescription,
  renderAsButton,
  builderRoutePath,
  className,
}) => {
  const navigate = useNavigate();
  const { width, ref } = useResizeDetector();
  const applyNarrowLayout = Boolean(width && width < 460);

  const content = (
    <FlexContainer direction="row" justifyContent="flex-start" alignItems="center" gap="md">
      <BuilderPromptIcon className={styles.icon} />
      <FlexContainer direction="column" gap="sm">
        <Text size="lg" as="span" className={classNames(styles.text, { [styles.buttonText]: renderAsButton })}>
          <FormattedMessage id="connectorBuilder.builderPrompt.title" />
        </Text>
        <Text size="sm" className={classNames(styles.description, { [styles.buttonText]: renderAsButton })}>
          <FormattedMessage
            id={
              shortDescription
                ? "connectorBuilder.builderPrompt.shortDescription"
                : "connectorBuilder.builderPrompt.description"
            }
            values={{
              adjective: (
                <Text as="span" bold gradient size="sm">
                  <FormattedMessage id="connectorBuilder.builderPrompt.adjective" />
                </Text>
              ),
              noun: (
                <Text as="span" bold size="sm" className={styles.description}>
                  <FormattedMessage id="connectorBuilder.builderPrompt.noun" />
                </Text>
              ),
            }}
          />
        </Text>
      </FlexContainer>
    </FlexContainer>
  );

  const navigateToBuilder = () => navigate(builderRoutePath);

  if (renderAsButton) {
    return (
      <button className={className} onClick={navigateToBuilder}>
        {content}
      </button>
    );
  }

  return (
    <FlexContainer
      className={className}
      direction={applyNarrowLayout ? "column" : "row"}
      alignItems="center"
      justifyContent="space-between"
      gap="md"
      ref={ref}
    >
      {content}
      <Button variant="secondary" onClick={navigateToBuilder} full={applyNarrowLayout}>
        <FormattedMessage id="connectorBuilder.builderPrompt.button" />
      </Button>
    </FlexContainer>
  );
};
