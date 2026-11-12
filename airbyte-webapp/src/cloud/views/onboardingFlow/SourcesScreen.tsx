import { useMutation } from "@tanstack/react-query";
import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { EmptyState } from "components/ui/EmptyState";
import { FlexContainer } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";

import { useSourceDefinitionList } from "core/api";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { isDefined } from "core/utils/common";

import { OnboardingActionBar } from "./OnboardingActionBar";
import { SOURCE_CATEGORIES } from "./onboardingConnectors";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";
import { SourceCategoryCard } from "./SourceCategoryCard";
import { SourceCheckbox } from "./SourceCheckbox";
import { isMissingDefinitionError, useSyncDraftSources } from "./useSyncDraftConnectors";
import { useTrackOnboardingAnswer } from "./useTrackOnboardingAnswer";

/** A new pick goes last, so the picks keep their order. */
const togglePick = (picks: string[], definitionId: string) =>
  picks.includes(definitionId) ? picks.filter((pick) => pick !== definitionId) : [...picks, definitionId];

/** A short search term matches hundreds of connectors, so the results card shows only the first ones. */
const MAX_SEARCH_RESULTS = 10;

/** Every word of the search term must be in the connector name, as in the connector picker. */
const searchSourceDefinitions = (definitions: SourceDefinitionRead[], searchTerm: string) => {
  const keywords = searchTerm.toLowerCase().split(" ").filter(Boolean);
  return definitions
    .filter(({ name }) => keywords.every((keyword) => name.toLowerCase().includes(keyword)))
    .slice(0, MAX_SEARCH_RESULTS);
};

interface SearchResultsProps {
  results: SourceDefinitionRead[];
  picks: string[];
  onToggle: (sourceDefinitionId: string) => void;
}

const SearchResults: React.FC<SearchResultsProps> = ({ results, picks, onToggle }) => {
  const { formatMessage } = useIntl();

  return (
    <Card title={formatMessage({ id: "onboardingFlow.sources.searchResults" })}>
      {results.length === 0 ? (
        <EmptyState
          icon="sleep"
          text={<FormattedMessage id="onboardingFlow.sources.noResults" />}
          description={<FormattedMessage id="onboardingFlow.sources.noResults.description" />}
        />
      ) : (
        <FlexContainer direction="column" gap="md">
          {results.map((definition) => (
            <SourceCheckbox
              key={definition.sourceDefinitionId}
              definition={definition}
              checked={picks.includes(definition.sourceDefinitionId)}
              onToggle={onToggle}
            />
          ))}
        </FlexContainer>
      )}
    </Card>
  );
};

interface SourcesScreenProps {
  picks: string[];
  onPicksChange: (picks: string[]) => void;
  onBack: () => void;
  onContinue: () => void;
  onSkip?: () => void;
}

export const SourcesScreen: React.FC<SourcesScreenProps> = ({ picks, onPicksChange, onBack, onContinue, onSkip }) => {
  const { formatMessage } = useIntl();
  const { sourceDefinitions, sourceDefinitionMap } = useSourceDefinitionList();
  const syncDraftSources = useSyncDraftSources();
  const trackAnswer = useTrackOnboardingAnswer();

  const continueWithSavedPicks = (savedPicks: string[]) => {
    trackAnswer("sources", { selection_order: savedPicks });
    onContinue();
  };

  const { mutate: saveSources, isLoading: isSaving } = useMutation(syncDraftSources, {
    onSuccess: (_result, savedPicks) => continueWithSavedPicks(savedPicks),
    useErrorBoundary: isMissingDefinitionError,
  });
  const [searchTerm, setSearchTerm] = useState("");

  const toggleSourcePick = (sourceDefinitionId: string) => onPicksChange(togglePick(picks, sourceDefinitionId));

  return (
    <OnboardingScreenLayout
      title={<FormattedMessage id="onboardingFlow.sources.title" />}
      actions={
        <OnboardingActionBar
          onSkip={onSkip}
          onBack={onBack}
          onContinue={picks.length > 0 ? () => saveSources(picks) : undefined}
          isSaving={isSaving}
        />
      }
    >
      <SearchInput
        value={searchTerm}
        onChange={setSearchTerm}
        placeholder={formatMessage({ id: "onboardingFlow.sources.searchPlaceholder" })}
      />
      {searchTerm && (
        <SearchResults
          results={searchSourceDefinitions(sourceDefinitions, searchTerm)}
          picks={picks}
          onToggle={toggleSourcePick}
        />
      )}
      <FlexContainer direction="column" gap="sm">
        {SOURCE_CATEGORIES.map(({ id, labelId, icon, sourceDefinitionIds }) => (
          <SourceCategoryCard
            key={id}
            labelId={labelId}
            icon={icon}
            definitions={sourceDefinitionIds
              .map((definitionId) => sourceDefinitionMap.get(definitionId))
              .filter(isDefined)}
            picks={picks}
            onToggle={toggleSourcePick}
          />
        ))}
      </FlexContainer>
    </OnboardingScreenLayout>
  );
};
