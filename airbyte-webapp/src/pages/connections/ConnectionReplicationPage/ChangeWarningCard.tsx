import { FormattedMessage, useIntl } from "react-intl";
import { match } from "ts-pattern";

import { Box } from "components/ui/Box";
import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { RadioButtonTiles } from "area/connection/components/CreateConnection/RadioButtonTiles";
import { links } from "core/utils/links";

import styles from "./ChangeWarningCard.module.scss";
import { ChangeItem, ChangeWarning } from "./connectionUpdateHelpers";

interface ChangeWarningCardProps {
  warning: ChangeWarning;
  affectedStreams: ChangeItem[];
  decision: "accept" | "reject";
  onDecision: (decision: "accept" | "reject") => void;
}

const ChangeSummary = ({ streams, warning }: { streams: ChangeItem[]; warning: ChangeWarning }) => {
  const { formatMessage } = useIntl();

  if (streams.length === 1) {
    const stream = streams[0];
    return (
      <Text>
        <FormattedMessage id={`connection.${warning}.summary.single`} values={{ streamName: stream.streamName }} />
      </Text>
    );
  }
  const label = formatMessage(
    { id: `connection.${warning}.summary.multiple` },
    { streamName: streams[0].streamName, count: streams.length }
  );
  return (
    <Collapsible label={label} className={styles.streamList__container}>
      <ul className={styles.streamList__list}>
        {streams.map((stream) => {
          return (
            <li key={stream.id}>
              <Text className={styles.semibold}>{stream.streamName}</Text>
            </li>
          );
        })}
      </ul>
    </Collapsible>
  );
};

const WarningBox = ({ warning }: { warning: ChangeWarning }) => {
  return match(warning)
    .with("fullRefreshHighFrequency", () => (
      <Message
        type="warning"
        text={
          <Text>
            <FormattedMessage id="connection.fullRefreshHighFrequency.warning" />
          </Text>
        }
      />
    ))
    .with("refresh", () => (
      <Message
        type="info"
        text={
          <Text>
            <FormattedMessage id="connection.refresh.info" />
          </Text>
        }
      />
    ))
    .with("clear", () => (
      <Message
        type="info"
        text={
          <Text>
            <FormattedMessage id="connection.clear.info" />
          </Text>
        }
      />
    ))
    .exhaustive();
};
export const ChangeWarningCard: React.FC<ChangeWarningCardProps> = ({
  warning,
  affectedStreams,
  decision,
  onDecision,
}) => {
  if (affectedStreams.length === 0) {
    return null;
  }

  const rejectRadioButton = match(warning)
    .with("fullRefreshHighFrequency", () => {
      return {
        label: (
          <Text>
            <FormattedMessage id="connection.fullRefreshHighFrequency.cancel" />
          </Text>
        ),
        description: (
          <Text size="xs">
            <FormattedMessage
              id="connection.fullRefreshHighFrequency.cancel.description"
              values={{
                lnk: (chunks) => <ExternalLink href={links.refreshes}>{chunks}</ExternalLink>,
              }}
            />
          </Text>
        ),
      };
    })
    .with("refresh", () => {
      return {
        label: (
          <Text>
            <FormattedMessage id="connection.refresh.saveAndSkip" />
          </Text>
        ),
        description: "",
      };
    })
    .with("clear", () => {
      return {
        label: (
          <Text>
            <FormattedMessage id="connection.clear.saveAndSkip" />
          </Text>
        ),
        description: (
          <Text size="xs">
            <FormattedMessage id="connection.clear.saveAndSkip.description" />
          </Text>
        ),
      };
    })
    .run();

  return (
    <FlexContainer
      className={styles.changeCard}
      direction="column"
      gap="lg"
      data-testid={`change-warning-card-${warning}`}
    >
      <ChangeSummary streams={affectedStreams} warning={warning} />
      <WarningBox warning={warning} />

      <Box pt="lg" data-testid={`change-warning-card-${warning}-decisions`}>
        <RadioButtonTiles
          light
          direction="column"
          options={[
            {
              value: "accept",
              label: (
                <Text>
                  <FormattedMessage id={`connection.${warning}.confirm`} />
                </Text>
              ),
              description: "",
              "data-testid": `radio-button-tile-${warning}-decision-accept`,
            },
            {
              value: "reject",
              ...rejectRadioButton,
              "data-testid": `radio-button-tile-${warning}-decision-reject`,
            },
          ]}
          selectedValue={decision}
          onSelectRadioButton={(value) => {
            onDecision(value);
          }}
          name={`${warning}-decision`}
          data-testid={`change-warning-card-${warning}-radio-tiles`}
        />
      </Box>
    </FlexContainer>
  );
};
