import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { HashingMapperConfigurationMethod } from "core/api/types/AirbyteClient";
import { FilterCondition } from "pages/connections/ConnectionMappingsPage/RowFilteringMapperForm";

import styles from "./AdditionalMappers.module.scss";
import { MapperTypeField } from "./MapperTypeField";
import { RowItem } from "./RowItem";

interface AdditionalMappersProps {
  streamIndex: number;
  fieldIndex: number;
}

export const AdditionalMappers: React.FC<AdditionalMappersProps> = ({ streamIndex, fieldIndex }) => {
  const {
    fields: additionalMappers,
    append: appendAdditionalMapper,
    remove: removeAdditionalMapper,
  } = useFieldArray<DataActivationConnectionFormValues, `streams.${number}.fields.${number}.additionalMappers`>({
    name: `streams.${streamIndex}.fields.${fieldIndex}.additionalMappers`,
  });

  return (
    <div className={styles.additionalMappers}>
      {additionalMappers.map((mapper, index) => (
        <AdditionalMapper
          key={mapper.id}
          streamIndex={streamIndex}
          fieldIndex={fieldIndex}
          additionalMapperIndex={index}
          removeAdditionalMapper={() => removeAdditionalMapper(index)}
        />
      ))}
      <div className={styles.additionalMappers__addButtonContainer}>
        <button
          className={styles.additionalMappers__addButton}
          type="button"
          onClick={() =>
            appendAdditionalMapper({
              type: "hashing",
              method: "MD5",
            })
          }
        >
          <Icon type="plus" size="xs" />
          <FormattedMessage id="connection.dataActivation.addFilter" />
        </button>
      </div>
    </div>
  );
};

interface AdditionalMapperProps {
  streamIndex: number;
  fieldIndex: number;
  additionalMapperIndex: number;
  removeAdditionalMapper: () => void;
}

const AdditionalMapper: React.FC<AdditionalMapperProps> = ({
  streamIndex,
  fieldIndex,
  additionalMapperIndex,
  removeAdditionalMapper,
}) => {
  const { formatMessage } = useIntl();
  const { control } = useFormContext<DataActivationConnectionFormValues>();
  const mapperType = useWatch({
    control,
    name: `streams.${streamIndex}.fields.${fieldIndex}.additionalMappers.${additionalMapperIndex}.type`,
  });

  return (
    <>
      <div className={styles.additionalMappers__nestingIcon}>
        <RowItem>
          <Icon type="nested" color="disabled" />
        </RowItem>
      </div>
      <FlexContainer gap="sm" alignItems="flex-start" wrap="wrap">
        <RowItem>
          <MapperTypeField
            name={`streams.${streamIndex}.fields.${fieldIndex}.additionalMappers.${additionalMapperIndex}`}
          />
        </RowItem>
        {mapperType === "hashing" && (
          <>
            <RowItem>
              <Text>
                <FormattedMessage id="connections.mappings.using" />
              </Text>
            </RowItem>
            <RowItem>
              <FormControl
                reserveSpaceForError={false}
                name={`streams.${streamIndex}.fields.${fieldIndex}.additionalMappers.${additionalMapperIndex}.method`}
                fieldType="dropdown"
                options={[
                  {
                    label: "MD5",
                    value: HashingMapperConfigurationMethod.MD5,
                  },
                  {
                    label: "SHA-256",
                    value: HashingMapperConfigurationMethod["SHA-256"],
                  },
                  {
                    label: "SHA-512",
                    value: HashingMapperConfigurationMethod["SHA-512"],
                  },
                ]}
              />
            </RowItem>
          </>
        )}
        {mapperType === "row-filtering" && (
          <>
            <RowItem>
              <FormControl
                reserveSpaceForError={false}
                name={`streams.${streamIndex}.fields.${fieldIndex}.additionalMappers.${additionalMapperIndex}.condition`}
                fieldType="dropdown"
                options={[
                  {
                    label: formatMessage({ id: "connections.mappings.rowFilter.in" }),
                    value: FilterCondition.IN,
                  },
                  {
                    label: formatMessage({ id: "connections.mappings.rowFilter.out" }),
                    value: FilterCondition.OUT,
                  },
                ]}
              />
            </RowItem>
            <RowItem>
              <Text>
                <FormattedMessage id="connections.mappings.ifTheValueEquals" />
              </Text>
            </RowItem>
            <RowItem width={160}>
              <FormControl
                fieldType="input"
                reserveSpaceForError={false}
                name={`streams.${streamIndex}.fields.${fieldIndex}.additionalMappers.${additionalMapperIndex}.comparisonValue`}
                placeholder={formatMessage({ id: "connections.mappings.value" })}
              />
            </RowItem>
          </>
        )}
        {mapperType === "encryption" && (
          <>
            <RowItem>
              <Text>
                <FormattedMessage id="connections.mappings.usingRsaAndKey" />
              </Text>
            </RowItem>
            <RowItem width={240}>
              <FormControl
                fieldType="input"
                reserveSpaceForError={false}
                placeholder={formatMessage({ id: "connections.mappings.encryption.publicKey" })}
                name={`streams.${streamIndex}.fields.${fieldIndex}.additionalMappers.${additionalMapperIndex}.publicKey`}
              />
            </RowItem>
          </>
        )}
        <RowItem>
          <Button variant="clear" onClick={removeAdditionalMapper} icon="trash" />
        </RowItem>
      </FlexContainer>
    </>
  );
};
