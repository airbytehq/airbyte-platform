import { ComponentProps, useEffect, useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import {
  appendChangesModes,
  pruneUnsupportedModes,
  replicateSourceModes,
} from "components/connection/ConnectionForm/preferredSyncModes";
import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { updateStreamSyncMode } from "components/connection/SyncCatalogTable/utils/updateStreamSyncMode";
import { ControlLabels } from "components/LabeledControl";
import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useGetDestinationDefinitionSpecification } from "core/api";
import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./SimplifiedSchemaQuestionnaire.module.scss";
import { SyncModeValue } from "../../SyncCatalogTable/components/SyncModeCell";

type Delivery = "replicateSource" | "appendChanges";
type IncrementOrRefresh = SyncMode;
type QuestionnaireOutcomes = Record<Delivery, Array<[SyncMode, DestinationSyncMode]>>;

const deliveryOptions: ComponentProps<typeof RadioButtonTiles<Delivery>>["options"] = [
  {
    value: "replicateSource",
    label: (
      <FormattedMessage
        id="connectionForm.questionnaire.delivery.replicateSource.title"
        values={{
          badge: (
            <Badge variant="blue" className={styles.badgeAlignment}>
              Recommended
            </Badge>
          ),
        }}
      />
    ),
    description: <FormattedMessage id="connectionForm.questionnaire.delivery.replicateSource.subtitle" />,
  },
  {
    value: "appendChanges",
    label: <FormattedMessage id="connectionForm.questionnaire.delivery.appendChanges.title" />,
    description: <FormattedMessage id="connectionForm.questionnaire.delivery.appendChanges.subtitle" />,
  },
];

const deletionRecordsOptions: ComponentProps<typeof RadioButtonTiles<IncrementOrRefresh>>["options"] = [
  {
    value: SyncMode.incremental,
    label: <FormattedMessage id="connectionForm.questionnaire.incrementOrRefresh.increment.title" />,
    description: <FormattedMessage id="connectionForm.questionnaire.incrementOrRefresh.increment.subtitle" />,
  },
  {
    value: SyncMode.full_refresh,
    label: <FormattedMessage id="connectionForm.questionnaire.incrementOrRefresh.refresh.title" />,
    description: <FormattedMessage id="connectionForm.questionnaire.incrementOrRefresh.refresh.subtitle" />,
    extra: (
      <Text color="blue" size="sm">
        <FlexContainer alignItems="flex-end" gap="xs">
          <Icon type="warningOutline" size="sm" />
          <FormattedMessage id="connectionForm.questionnaire.incrementOrRefresh.refresh.warning" />
        </FlexContainer>
      </Text>
    ),
  },
];

export const getEnforcedDelivery = (outcomes: QuestionnaireOutcomes): Delivery | undefined => {
  if (outcomes.replicateSource.length === 0) {
    // there are no replicateSource choices; return appendChanges if present otherwise choice valid
    return outcomes.appendChanges.length > 0 ? "appendChanges" : undefined;
  } else if (outcomes.appendChanges.length === 0) {
    return "replicateSource"; // has replicateSource but no appendChanges, pre-select replicateSource
  } else if (
    outcomes.replicateSource.length === 1 &&
    outcomes.appendChanges.length === 1 &&
    outcomes.replicateSource[0][0] === outcomes.appendChanges[0][0] &&
    outcomes.replicateSource[0][1] === outcomes.appendChanges[0][1]
  ) {
    // has replicateSource and has appendChanges; both are [length=1] and have the same SyncMode
    return "replicateSource"; // which value is returned doesn't matter, so replicateSource it is
  }

  // multiple options, pre-select nothing so user can choose
  return undefined;
};

export const getEnforcedIncrementOrRefresh = (supportedSyncModes: SyncMode[]) => {
  return supportedSyncModes.length === 1 ? supportedSyncModes[0] : undefined;
};

export const SimplifiedSchemaQuestionnaire = () => {
  const analyticsService = useAnalyticsService();
  const { connection } = useConnectionFormService();
  const { supportedDestinationSyncModes } = useGetDestinationDefinitionSpecification(
    connection.destination.destinationId
  );

  const supportedSyncModes: SyncMode[] = useMemo(() => {
    const foundModes = new Set<SyncMode>();
    for (let i = 0; i < connection.syncCatalog.streams.length; i++) {
      const stream = connection.syncCatalog.streams[i];
      stream.stream?.supportedSyncModes?.forEach((mode) => foundModes.add(mode));
    }
    return Array.from(foundModes);
  }, [connection.syncCatalog.streams]);

  const questionnaireOutcomes = useMemo<QuestionnaireOutcomes>(
    () => ({
      replicateSource: pruneUnsupportedModes(replicateSourceModes, supportedSyncModes, supportedDestinationSyncModes),
      appendChanges: pruneUnsupportedModes(appendChangesModes, supportedSyncModes, supportedDestinationSyncModes),
    }),
    [supportedSyncModes, supportedDestinationSyncModes]
  );

  const enforcedSelectedDelivery = getEnforcedDelivery(questionnaireOutcomes);
  const enforcedIncrementOrRefresh = getEnforcedIncrementOrRefresh(supportedSyncModes);

  const [selectedDelivery, _setSelectedDelivery] = useState<Delivery>(enforcedSelectedDelivery ?? "replicateSource");
  const [selectedIncrementOrRefresh, _setSelectedIncrementOrRefresh] = useState<IncrementOrRefresh | undefined>(
    enforcedIncrementOrRefresh
  );

  const setSelectedDelivery: typeof _setSelectedDelivery = (value) => {
    analyticsService.track(Namespace.SYNC_QUESTIONNAIRE, Action.ANSWERED, {
      actionDescription: "First question has been answered",
      question: "delivery",
      answer: value,
    });

    _setSelectedDelivery(value);
    if (value === "replicateSource") {
      // clear any user-provided answer for the second question when switching to replicateSource
      // this is purely a UX decision
      setSelectedIncrementOrRefresh(enforcedIncrementOrRefresh, { automatedAction: true });
    }
  };

  const setSelectedIncrementOrRefresh = (
    value: SyncMode | undefined,
    { automatedAction }: { automatedAction?: boolean } = { automatedAction: false }
  ) => {
    if (!automatedAction) {
      analyticsService.track(Namespace.SYNC_QUESTIONNAIRE, Action.ANSWERED, {
        actionDescription: "Second question has been answered",
        question: "all_or_some",
        answer: value,
      });
    }

    _setSelectedIncrementOrRefresh(value);
  };

  const selectedModes = useMemo<SyncModeValue[]>(() => {
    if (selectedDelivery === "replicateSource") {
      return questionnaireOutcomes.replicateSource.map(([syncMode, destinationSyncMode]) => {
        return {
          syncMode,
          destinationSyncMode,
        };
      });
    } else if (selectedDelivery === "appendChanges" && selectedIncrementOrRefresh) {
      return [
        {
          syncMode: selectedIncrementOrRefresh,
          destinationSyncMode: DestinationSyncMode.append,
        },
      ];
    }
    return [];
  }, [selectedDelivery, questionnaireOutcomes.replicateSource, selectedIncrementOrRefresh]);

  // when a sync mode is selected, choose it for all streams
  const { trigger, getValues, setValue } = useFormContext<FormConnectionFormValues>();
  useEffect(() => {
    if (!selectedModes.length) {
      return;
    }

    const currentFields = getValues("syncCatalog.streams");
    const nextFields = currentFields.map((field) => {
      for (let i = 0; i < selectedModes.length; i++) {
        const { syncMode, destinationSyncMode } = selectedModes[i];

        if (field?.stream && field?.config) {
          if (!field.stream?.supportedSyncModes?.includes(syncMode)) {
            continue;
          }

          const nextConfig = updateStreamSyncMode(field.stream, field.config, {
            syncMode,
            destinationSyncMode,
          });
          return {
            ...field,
            config: nextConfig,
          };
        }
      }
      return field;
    });
    setValue("syncCatalog.streams", nextFields);
    trigger("syncCatalog.streams");

    analyticsService.track(Namespace.SYNC_QUESTIONNAIRE, Action.APPLIED, {
      actionDescription: "Questionnaire has applied a sync mode",
      delivery: selectedDelivery,
      all_or_some: selectedIncrementOrRefresh,
    });
  }, [setValue, trigger, getValues, selectedDelivery, selectedIncrementOrRefresh, selectedModes, analyticsService]);

  const showFirstQuestion = enforcedSelectedDelivery == null;
  const showSecondQuestion = enforcedIncrementOrRefresh == null && selectedDelivery === "appendChanges";

  useEffect(() => {
    if (showFirstQuestion) {
      analyticsService.track(Namespace.SYNC_QUESTIONNAIRE, Action.DISPLAYED, {
        actionDescription: "First question has been shown to the user",
        question: "delivery",
      });
    }
  }, [showFirstQuestion, analyticsService]);

  useEffect(() => {
    if (showSecondQuestion) {
      analyticsService.track(Namespace.SYNC_QUESTIONNAIRE, Action.DISPLAYED, {
        actionDescription: "Second question has been shown to the user",
        question: "all_or_some",
      });
    }
  }, [showSecondQuestion, analyticsService]);

  return (
    <FlexContainer direction="column" gap="md">
      {showFirstQuestion && (
        <FlexContainer direction="column">
          <ControlLabels
            label={
              <FlexContainer direction="column">
                <Text as="div">
                  <FormattedMessage id="connectionForm.questionnaire.delivery" />
                </Text>
              </FlexContainer>
            }
          />
          <RadioButtonTiles
            direction="row"
            name="delivery"
            options={deliveryOptions}
            selectedValue={selectedDelivery ?? ""}
            onSelectRadioButton={setSelectedDelivery}
          />
        </FlexContainer>
      )}

      <div className={showSecondQuestion ? styles.expandedQuestion : styles.collapsedQuestion}>
        <Box mt="md">
          <FlexContainer direction="column">
            <ControlLabels
              label={
                <FlexContainer direction="column">
                  <Text>
                    <FormattedMessage id="connectionForm.questionnaire.incrementOrRefresh" />
                  </Text>
                </FlexContainer>
              }
            />
            <RadioButtonTiles
              direction="row"
              name="delectedRecords"
              options={deletionRecordsOptions}
              selectedValue={selectedIncrementOrRefresh ?? ""}
              onSelectRadioButton={setSelectedIncrementOrRefresh}
            />
          </FlexContainer>
        </Box>
      </div>
    </FlexContainer>
  );
};
