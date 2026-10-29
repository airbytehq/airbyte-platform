import { useEffect, useState } from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { FormBlock, SectionDetails } from "core/form/types";

import { getSectionCheckValues } from "./sectionCheckValues";
import { isSameOrChildPath } from "./sectionPaths";
import { useConnectorForm } from "../../connectorFormContext";
import { ConnectorFormValues } from "../../types";

interface SectionCheckError {
  sectionId: string;
  /** Undefined when the check did not run because the user has not authenticated with OAuth yet. */
  error?: Error;
}

interface UseSectionChecksProps {
  sections: SectionDetails[];
  blocksBySection: FormBlock[][];
  pathsBySection: string[][];
  onCheckSection?: (values: ConnectorFormValues) => Promise<void>;
}

// Returns the same set when it holds none of the ids, so that typing in a field does not re-render the form
const withoutSections = (passedSectionIds: ReadonlySet<string>, sectionIds: string[]) =>
  sectionIds.some((sectionId) => passedSectionIds.has(sectionId))
    ? new Set(Array.from(passedSectionIds).filter((sectionId) => !sectionIds.includes(sectionId)))
    : passedSectionIds;

export const useSectionChecks = ({
  sections,
  blocksBySection,
  pathsBySection,
  onCheckSection,
}: UseSectionChecksProps) => {
  const { formatMessage } = useIntl();
  const { getValues, watch } = useFormContext<ConnectorFormValues>();
  const { castValues } = useConnectorForm();
  const [checkingSectionId, setCheckingSectionId] = useState<string>();
  const [sectionCheckError, setSectionCheckError] = useState<SectionCheckError>();
  const [passedSectionIds, setPassedSectionIds] = useState<ReadonlySet<string>>(() => new Set());

  // A passed check no longer holds once a field it checked changes. The check of a section includes the fields of the
  // sections before it, so a change also clears the checks of the later sections. A change without a name, such as a
  // reset, or to an object that holds fields, such as the whole connector configuration, changes all of its fields.
  useEffect(() => {
    const subscription = watch((_values, { name }) => {
      const isChangedPath = (path: string) => !name || isSameOrChildPath(path, name) || isSameOrChildPath(name, path);
      const changedSectionIndex = pathsBySection.findIndex((sectionPaths) => sectionPaths.some(isChangedPath));
      if (changedSectionIndex === -1) {
        return;
      }
      const outdatedSectionIds = sections.slice(changedSectionIndex).map((section) => section.id);
      setPassedSectionIds((sectionIds) => withoutSections(sectionIds, outdatedSectionIds));
    });
    return () => subscription.unsubscribe();
  }, [pathsBySection, sections, watch]);

  // A check that passed is not run again until one of its fields changes
  const needsSectionCheck = (section: SectionDetails) =>
    onCheckSection !== undefined && section.check === true && !passedSectionIds.has(section.id);

  const runSectionCheck = async (sectionIndex: number): Promise<boolean> => {
    const section = sections[sectionIndex];
    if (!onCheckSection || !needsSectionCheck(section)) {
      return true;
    }
    setSectionCheckError(undefined);
    setCheckingSectionId(section.id);
    try {
      await onCheckSection(getSectionCheckValues(castValues(getValues()), sections, blocksBySection, sectionIndex));
      setPassedSectionIds((sectionIds) => new Set(sectionIds).add(section.id));
      return true;
    } catch (e) {
      setSectionCheckError({
        sectionId: section.id,
        error: e instanceof Error ? e : new Error(formatMessage({ id: "connector.check.failed" })),
      });
      return false;
    } finally {
      setCheckingSectionId(undefined);
    }
  };

  const requireAuthentication = (sectionId: string) => setSectionCheckError({ sectionId });

  return {
    isCheckPassed: (sectionId: string) => passedSectionIds.has(sectionId),
    isChecking: checkingSectionId !== undefined,
    checkingSectionId,
    sectionCheckError,
    needsSectionCheck,
    runSectionCheck,
    requireAuthentication,
  };
};
