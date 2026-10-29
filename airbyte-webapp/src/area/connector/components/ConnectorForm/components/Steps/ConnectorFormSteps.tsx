import uniq from "lodash/uniq";
import React, { useEffect, useId, useMemo, useRef, useState } from "react";
import { FieldPath, useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Message } from "components/ui/Message";
import { NumberBadge } from "components/ui/NumberBadge";
import { StepStatus } from "components/ui/StepsIndicators/StepsIndicators";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { JobFailure } from "area/connection/components/JobFailure";
import { CommandErrorWithJobInfo } from "core/api";
import { FormBlock, GroupDetails, SectionDetails } from "core/form/types";
import { generateMessageFromError } from "core/utils/errorStatusMessage";

import styles from "./ConnectorFormSteps.module.scss";
import { getErrorPaths, getSectionPaths, isPathInSection, splitSections } from "./sectionPaths";
import { SectionProgressProvider } from "./SectionProgress";
import { useSectionChecks } from "./useSectionChecks";
import { useConnectorForm } from "../../connectorFormContext";
import { getEmptyRequiredFields, useFillRequiredFieldsMessage } from "../../requiredFields";
import { ConnectorFormValues } from "../../types";
import { useAuthentication } from "../../useAuthentication";
import { FormSection } from "../Sections/FormSection";

interface ConnectorFormStepsProps {
  blocks: FormBlock[];
  groupStructure?: GroupDetails[];
  sections: SectionDetails[];
  disabled?: boolean;
  header?: React.ReactNode;
  /** The title of the form. As in the one-page form, the description shows only with a title. */
  title?: string;
  description?: React.ReactNode;
  /** The form footer. It reads the active section through `useSectionProgress`. */
  footer?: React.ReactNode;
  /**
   * Create mode only: runs a connector check before the user leaves a section that is not the last one and declares
   * `check: true`. The connector config holds the fields of the section and of the sections before it, and the section
   * id under `check-section`. Resolves when the check passed. Rejects with the error to show in the section when the
   * check fails.
   */
  onCheckSection?: (values: ConnectorFormValues) => Promise<void>;
}

export const ConnectorFormSteps: React.FC<ConnectorFormStepsProps> = ({
  blocks,
  groupStructure,
  sections,
  disabled,
  header,
  title,
  description,
  footer,
  onCheckSection,
}) => {
  const { formatMessage } = useIntl();
  const { trigger, getFieldState } = useFormContext<ConnectorFormValues>();
  const { errors, submitCount } = useFormState<ConnectorFormValues>();
  const { isHiddenAuthField, shouldShowAuthButton } = useAuthentication();
  const [activeSectionIndex, setActiveSectionIndex] = useState(0);
  const isContinuing = useRef(false);

  const blocksBySection = useMemo(() => splitSections(blocks, sections), [blocks, sections]);
  const pathsBySection = useMemo(() => blocksBySection.map(getSectionPaths), [blocksBySection]);
  const authSectionIndex = blocksBySection.findIndex((sectionBlocks) =>
    sectionBlocks.some((block) => shouldShowAuthButton(block.path))
  );

  const {
    isCheckPassed,
    isChecking,
    checkingSectionId,
    sectionCheckError,
    needsSectionCheck,
    runSectionCheck,
    requireAuthentication,
  } = useSectionChecks({ sections, blocksBySection, pathsBySection, onCheckSection });

  const continueToNextSection = async () => {
    if (isContinuing.current) {
      return;
    }
    isContinuing.current = true;
    try {
      const sectionIndex = activeSectionIndex;
      const sectionPaths = pathsBySection[sectionIndex];
      const isSectionValid = await trigger(sectionPaths as Array<FieldPath<ConnectorFormValues>>, {
        shouldFocus: true,
      });
      const errorPaths = isSectionValid
        ? []
        : sectionPaths.flatMap((path) =>
            getErrorPaths(getFieldState(path as FieldPath<ConnectorFormValues>).error, path)
          );
      const visibleErrorPaths = errorPaths
        // Fields filled by OAuth are not rendered, so their errors would block the section without a visible reason.
        // The final submit still validates them and shows the error on the OAuth button.
        .filter((path) => !isHiddenAuthField(path));
      if (visibleErrorPaths.length > 0) {
        return;
      }

      // Only errors in fields that OAuth fills are left. A check without those fields can only fail, so the user
      // authenticates first.
      const section = sections[sectionIndex];
      if (errorPaths.length > 0 && needsSectionCheck(section)) {
        requireAuthentication(section.id);
        return;
      }

      const isSectionCheckPassed = await runSectionCheck(sectionIndex);
      if (!isSectionCheckPassed) {
        return;
      }
      setActiveSectionIndex(sectionIndex + 1);
    } finally {
      isContinuing.current = false;
    }
  };

  // A failed final submit can come from a field in an earlier section, which is hidden, so open that section
  const handledSubmitCount = useRef(submitCount);
  useEffect(() => {
    if (submitCount === handledSubmitCount.current) {
      return;
    }
    handledSubmitCount.current = submitCount;

    // An error in a field that OAuth fills opens the section with the OAuth button. The button has a block of its own
    // only at the root of the connector configuration. Otherwise it renders in the object that holds those fields.
    const sectionIndexesWithErrors = getErrorPaths(errors, "")
      .map((path) =>
        isHiddenAuthField(path) && authSectionIndex !== -1
          ? authSectionIndex
          : pathsBySection.findIndex((sectionPaths) => isPathInSection(sectionPaths, path))
      )
      .filter((sectionIndex) => sectionIndex !== -1);
    if (sectionIndexesWithErrors.length === 0) {
      return;
    }

    const firstSectionIndexWithError = Math.min(...sectionIndexesWithErrors);
    if (firstSectionIndexWithError < activeSectionIndex) {
      setActiveSectionIndex(firstSectionIndexWithError);
    }
  }, [activeSectionIndex, authSectionIndex, errors, isHiddenAuthField, pathsBySection, submitCount]);

  // Moves focus to the title of the section that opens, but not to the first section when the form renders
  const sectionTitles = useRef<Array<HTMLElement | null>>([]);
  const focusedSectionIndex = useRef(activeSectionIndex);
  useEffect(() => {
    if (activeSectionIndex === focusedSectionIndex.current) {
      return;
    }
    focusedSectionIndex.current = activeSectionIndex;

    const sectionTitle = sectionTitles.current[activeSectionIndex];
    sectionTitle?.focus();
    sectionTitle?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [activeSectionIndex]);

  // Moves focus to the message of a section check that failed or did not run, so that keyboard and screen reader users
  // land on it. The Continue button had focus, but it is disabled while the check runs.
  const sectionCheckErrorMessage = useRef<HTMLDivElement>(null);
  useEffect(() => {
    sectionCheckErrorMessage.current?.focus();
  }, [sectionCheckError]);

  const isLastSectionActive = activeSectionIndex === sections.length - 1;
  const sectionProgress = useMemo(
    () => ({ activeSectionIndex, sectionCount: sections.length }),
    [activeSectionIndex, sections.length]
  );

  return (
    <FlexContainer direction="column" gap="xl">
      {title ? (
        <Card title={title} description={description} titleWithBottomBorder>
          {header}
        </Card>
      ) : (
        header
      )}
      {sections.map((section, sectionIndex) => {
        const status =
          sectionIndex < activeSectionIndex
            ? StepStatus.COMPLETE
            : sectionIndex === activeSectionIndex
            ? StepStatus.ACTIVE
            : StepStatus.INCOMPLETE;
        const checkError = sectionCheckError?.sectionId === section.id ? sectionCheckError : undefined;
        const checkErrorJobInfo = CommandErrorWithJobInfo.getJobInfo(checkError?.error ?? null);

        return (
          <Card key={section.id} dataTestId={`connector-form-section-${section.id}`}>
            <FlexContainer direction="column" gap="xl">
              <StepHeader
                titleRef={(element) => {
                  sectionTitles.current[sectionIndex] = element;
                }}
                section={section}
                stepNumber={sectionIndex + 1}
                status={status}
                isCheckPassed={isCheckPassed(section.id)}
                isEditDisabled={disabled || isChecking}
                onEdit={() => setActiveSectionIndex(sectionIndex)}
              />
              <div hidden={status !== StepStatus.ACTIVE}>
                <FlexContainer direction="column" gap="xl">
                  <FormSection
                    rootLevel
                    insideSectionCard
                    blocks={blocksBySection[sectionIndex]}
                    groupStructure={groupStructure}
                    disabled={disabled || checkingSectionId === section.id}
                  />
                  {status === StepStatus.ACTIVE && checkError && (
                    <div ref={sectionCheckErrorMessage} tabIndex={-1}>
                      {checkError.error ? (
                        <JobFailure
                          fallbackMessage={generateMessageFromError(checkError.error, formatMessage)}
                          id={checkErrorJobInfo?.id}
                          configType={checkErrorJobInfo?.configType}
                          logs={checkErrorJobInfo?.logs}
                          failureReason={checkErrorJobInfo?.failureReason}
                        />
                      ) : (
                        <Message type="error" text={<FormattedMessage id="connectorForm.authenticate.required" />} />
                      )}
                    </div>
                  )}
                  {status === StepStatus.ACTIVE && (
                    <StepNavigation
                      sectionTitle={section.title}
                      sectionPaths={pathsBySection[sectionIndex]}
                      isCheckRunning={checkingSectionId === section.id}
                      isDisabled={disabled || isChecking}
                      onBack={sectionIndex > 0 ? () => setActiveSectionIndex(sectionIndex - 1) : undefined}
                      onContinue={isLastSectionActive ? undefined : continueToNextSection}
                    />
                  )}
                </FlexContainer>
              </div>
            </FlexContainer>
          </Card>
        );
      })}
      <SectionProgressProvider value={sectionProgress}>{footer}</SectionProgressProvider>
    </FlexContainer>
  );
};

interface StepNavigationProps {
  sectionTitle: string;
  sectionPaths: string[];
  isCheckRunning: boolean;
  isDisabled: boolean;
  /** Undefined on the first section. */
  onBack?: () => void;
  /** Undefined on the last section, which the form submit completes. */
  onContinue?: () => void;
}

const StepNavigation: React.FC<StepNavigationProps> = ({
  sectionTitle,
  sectionPaths,
  isCheckRunning,
  isDisabled,
  onBack,
  onContinue,
}) => (
  <FlexContainer justifyContent="space-between" alignItems="center">
    {onBack ? (
      <Button type="button" variant="secondary" icon="chevronLeft" iconSize="xs" onClick={onBack} disabled={isDisabled}>
        <FormattedMessage id="connectorForm.sections.back" />
      </Button>
    ) : (
      <span />
    )}
    {onContinue && (
      <FlexContainer alignItems="center" gap="md">
        <Text as="span" color="grey" role="status">
          {isCheckRunning && <FormattedMessage id="connectorForm.sections.checking" values={{ title: sectionTitle }} />}
        </Text>
        <ContinueButton
          sectionPaths={sectionPaths}
          isCheckRunning={isCheckRunning}
          isDisabled={isDisabled}
          onContinue={onContinue}
        />
      </FlexContainer>
    )}
  </FlexContainer>
);

interface ContinueButtonProps {
  sectionPaths: string[];
  isCheckRunning: boolean;
  isDisabled: boolean;
  onContinue: () => void;
}

/** Yup writes array items as `streams[0]`, react-hook-form as `streams.0`. */
const toFormPath = (validationPath: string) => validationPath.replace(/\[(\d+)\]/g, ".$1");

// A separate component, so that watching every value re-renders only the button and not the section.
const ContinueButton: React.FC<ContinueButtonProps> = ({ sectionPaths, isCheckRunning, isDisabled, onContinue }) => {
  const values = useWatch<ConnectorFormValues>();
  const { validationSchema } = useConnectorForm();
  const { isHiddenAuthField } = useAuthentication();
  const describedBy = useId();

  // Fields that OAuth fills do not block the section, the same as on continue
  const emptyRequiredFields = useMemo(
    () =>
      uniq(
        getEmptyRequiredFields(validationSchema, values)
          .filter(({ path }) => !isHiddenAuthField(path) && isPathInSection(sectionPaths, toFormPath(path)))
          .map(({ label }) => label)
      ),
    [isHiddenAuthField, sectionPaths, validationSchema, values]
  );
  const fillRequiredFieldsMessage = useFillRequiredFieldsMessage(
    emptyRequiredFields,
    "connectorForm.sections.fillRequiredFields"
  );

  return (
    <>
      <Tooltip
        control={
          <Button
            type="button"
            onClick={onContinue}
            isLoading={isCheckRunning}
            disabled={isDisabled || fillRequiredFieldsMessage !== undefined}
            aria-describedby={fillRequiredFieldsMessage && describedBy}
          >
            <FormattedMessage id="form.continue" />
          </Button>
        }
        disabled={!fillRequiredFieldsMessage}
        placement="top"
        cursor="not-allowed"
      >
        {fillRequiredFieldsMessage?.tooltip}
      </Tooltip>
      {/* The screen-reader text for the disabled button, which cannot take focus to show the tooltip */}
      {fillRequiredFieldsMessage && (
        <span id={describedBy} className={styles.visuallyHidden}>
          {fillRequiredFieldsMessage.description}
        </span>
      )}
    </>
  );
};

interface StepHeaderProps {
  section: SectionDetails;
  stepNumber: number;
  status: StepStatus;
  isCheckPassed: boolean;
  isEditDisabled: boolean;
  onEdit: () => void;
  /** Receives the title element, which takes focus when the section opens. */
  titleRef: (element: HTMLElement | null) => void;
}

const StepHeader: React.FC<StepHeaderProps> = ({
  section,
  stepNumber,
  status,
  isCheckPassed,
  isEditDisabled,
  onEdit,
  titleRef,
}) => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer alignItems="center" justifyContent="space-between">
      <FlexContainer alignItems="center" gap="md">
        {status === StepStatus.COMPLETE ? (
          <Icon type="checkCircle" color="success" />
        ) : (
          <NumberBadge
            value={stepNumber}
            color={status === StepStatus.ACTIVE ? "blue" : "grey"}
            outline={status !== StepStatus.ACTIVE}
          />
        )}
        <FlexContainer direction="column" gap="xs">
          <FlexContainer alignItems="center" gap="md">
            <Text
              as="h2"
              size="lg"
              bold
              color={status === StepStatus.INCOMPLETE ? "grey" : "darkBlue"}
              ref={titleRef}
              tabIndex={-1}
              aria-current={status === StepStatus.ACTIVE ? "step" : undefined}
            >
              {section.title}
              {status !== StepStatus.INCOMPLETE && (
                <span className={styles.visuallyHidden}>
                  {" "}
                  <FormattedMessage
                    id={
                      status === StepStatus.COMPLETE
                        ? "connectorForm.sections.stepCompleted"
                        : "connectorForm.sections.stepCurrent"
                    }
                  />
                </span>
              )}
            </Text>
            {status === StepStatus.COMPLETE && isCheckPassed && (
              <Badge variant="green" uppercase={false} data-testid="connector-form-section-check-passed">
                <FormattedMessage id="connectorForm.sections.checkPassed" />
              </Badge>
            )}
          </FlexContainer>
          {status === StepStatus.ACTIVE && section.description && <Text color="grey">{section.description}</Text>}
        </FlexContainer>
      </FlexContainer>
      {status === StepStatus.COMPLETE && (
        <Button
          type="button"
          variant="secondary"
          size="xs"
          onClick={onEdit}
          disabled={isEditDisabled}
          aria-label={formatMessage({ id: "connectorForm.sections.edit" }, { title: section.title })}
        >
          <FormattedMessage id="form.edit" />
        </Button>
      )}
    </FlexContainer>
  );
};
