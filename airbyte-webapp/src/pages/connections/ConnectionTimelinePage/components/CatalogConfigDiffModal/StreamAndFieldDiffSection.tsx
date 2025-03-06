import classNames from "classnames";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { StreamFieldStatusChanged } from "core/api/types/AirbyteClient";

import styles from "./StreamAndFieldDiffSection.module.scss";
export interface StreamAndFieldDiffSectionProps {
  streams: StreamFieldStatusChanged[];
  fields: StreamFieldStatusChanged[];
  messageId: string;
  iconType: "plus" | "minus";
  iconColor: "success" | "error";
  backgroundColor?: "enabled" | "disabled";
}

export const StreamAndFieldDiffSection: React.FC<StreamAndFieldDiffSectionProps> = ({
  streams,
  fields,
  messageId,
  iconType,
  iconColor,
  backgroundColor,
}) => {
  const { formatMessage } = useIntl();
  const fieldCount = fields.reduce((total, stream) => total + (stream.fields?.length ?? 0), 0);

  if (streams && streams.length === 0 && fields && fields.length === 0) {
    return null;
  }

  return (
    <FlexContainer direction="column" gap="xs">
      <Text>
        {formatMessage(
          { id: messageId },
          {
            streamCount: streams.length,
            fieldCount,
            bothPresent: Boolean(streams.length && fieldCount),
            totalCount: streams.length + fieldCount,
          }
        )}
      </Text>
      <FlexContainer direction="column" gap="xs">
        {streams.length ? (
          <FlexContainer direction="column" gap="xs">
            {streams.map((stream) => (
              <FlexContainer
                alignItems="center"
                gap="xs"
                key={stream.streamName}
                className={classNames(
                  styles.diffSection__stream,
                  backgroundColor && styles[`diffSection--${backgroundColor}`]
                )}
              >
                <Icon type={iconType} color={iconColor} size="xs" />
                <Text color="grey500">{stream.streamName}</Text>
              </FlexContainer>
            ))}
          </FlexContainer>
        ) : null}
        {fields.length
          ? fields.map((stream) => (
              <FlexContainer gap="xs" direction="column" key={stream.streamName}>
                <Text color="grey500" className={styles.diffSection__streamName}>
                  {stream.streamName}
                </Text>
                <FlexContainer direction="column" gap="xs">
                  {stream.fields?.map((field) => (
                    <FlexContainer
                      gap="xs"
                      alignItems="center"
                      key={field}
                      className={classNames(
                        styles.diffSection__field,
                        backgroundColor && styles[`diffSection--${backgroundColor}`]
                      )}
                    >
                      <Icon type={iconType} color={iconColor} size="xs" />
                      <Text color="grey500">{`${stream.streamName}.${field}`}</Text>
                    </FlexContainer>
                  ))}
                </FlexContainer>
              </FlexContainer>
            ))
          : null}
      </FlexContainer>
    </FlexContainer>
  );
};
