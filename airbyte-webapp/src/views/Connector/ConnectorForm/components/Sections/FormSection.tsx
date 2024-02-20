import React from "react";
import { useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { ArrayOfObjectsSection } from "area/connector/components/ArrayOfObjectsSection";
import { FormBlock, GroupDetails } from "core/form/types";

import { AuthSection } from "./auth/AuthSection";
import { ConditionSection } from "./ConditionSection";
import { PropertySection } from "./PropertySection";
import { SectionContainer } from "./SectionContainer";
import { DisplayType, useGroupsAndSections } from "./useGroupsAndSections";
import { useAuthentication } from "../../useAuthentication";

interface FormNodeProps {
  formField: FormBlock;
  disabled?: boolean;
  sectionPath: string;
}

const FormNode: React.FC<FormNodeProps> = ({ sectionPath, formField, disabled }) => {
  if (formField._type === "formGroup") {
    return <FormSection path={sectionPath} blocks={formField.properties} disabled={disabled} />;
  } else if (formField._type === "formCondition") {
    return <ConditionSection path={sectionPath} formField={formField} disabled={disabled} />;
  } else if (formField._type === "objectArray") {
    return <ArrayOfObjectsSection path={sectionPath} formField={formField} disabled={disabled} />;
  } else if (formField.const !== undefined) {
    return null;
  }
  return (
    <SectionContainer>
      <PropertySection property={formField} path={sectionPath} disabled={disabled} />
    </SectionContainer>
  );
};

interface FormSectionProps {
  blocks: FormBlock[] | FormBlock;
  groupStructure?: GroupDetails[];
  path?: string;
  skipAppend?: boolean;
  rootLevel?: boolean;
  headerBlock?: {
    elements: React.ReactNode;
    title?: string;
    description?: React.ReactNode;
  };
  disabled?: boolean;
}

export const FormSection: React.FC<FormSectionProps> = ({
  blocks = [],
  groupStructure = [],
  path,
  skipAppend,
  disabled,
  rootLevel,
  headerBlock,
}) => {
  const { shouldShowAuthButton } = useAuthentication();

  const groups = useGroupsAndSections(blocks, groupStructure, Boolean(rootLevel));
  const groupElements = groups.map((sectionGroup, index) => {
    const collapsedNamedGroup = Boolean(
      sectionGroup.sections.length === 1 &&
        sectionGroup.sections[0].displayType === "collapsed-group" &&
        sectionGroup.title
    );
    const sectionElements = sectionGroup.sections.map((section, index) => (
      <SubSection
        label={collapsedNamedGroup ? sectionGroup.title : undefined}
        displayType={section.displayType}
        hasError={section.hasError}
        key={index}
        initiallyOpen={section.initiallyOpen}
      >
        {section.blocks.map((formField) => {
          const sectionPath = path ? (skipAppend ? path : `${path}.${formField.fieldKey}`) : formField.path;

          return (
            <React.Fragment key={formField.path}>
              {/*
                If the auth button should be rendered here, do so. In addition to the check useAuthentication does
                we also need to check if the formField type is not a `formCondition`. We render a lot of OAuth buttons
                in conditional fields in which case the path they should be rendered is the path of the conditional itself.
                For conditional fields we're rendering this component twice, once "outside" of the conditional, which causes
                the actual conditional frame to be rendered and once inside the conditional to render the actual content.
                Since we want to only render the auth button inside the conditional and not above it, we filter out the cases
                where the formField._type is formCondition, which will be the "outside rendering".
               */}
              {shouldShowAuthButton(sectionPath) && formField._type !== "formCondition" && <AuthSection />}
              <FormNode sectionPath={sectionPath} formField={formField} disabled={disabled} />
            </React.Fragment>
          );
        })}
      </SubSection>
    ));

    if (!rootLevel) {
      // visible cards are only rendered on the top level
      return sectionElements;
    }

    if (index === 0 && headerBlock) {
      // if a headerBlock is defined, the first card gets it assigned
      // TODO if the type dropdown gets removed from the form, this case can probably be removed
      return (
        <Card key={index} title={headerBlock.title} description={headerBlock.description} titleWithBottomBorder>
          {headerBlock.elements}
          {sectionElements}
        </Card>
      );
    }

    return (
      <Card key={index}>
        <FlexContainer direction="column" gap="xl">
          {sectionGroup.title && !collapsedNamedGroup && (
            <Heading as="h2" size="sm">
              {sectionGroup.title}
            </Heading>
          )}
          <FlexItem>{sectionElements}</FlexItem>
        </FlexContainer>
      </Card>
    );
  });

  if (!rootLevel) {
    // if its not on the root level, there is only one group which can be rendered directly
    return <>{groupElements}</>;
  }

  // on the top level, render individual cards in a flex container to get gaps between them
  return (
    <FlexContainer direction="column" gap="xl">
      {groupElements}
    </FlexContainer>
  );
};

interface SubSectionProps {
  displayType: DisplayType;
  hasError?: boolean;
  label?: string;
  initiallyOpen?: boolean;
}

const SubSection: React.FC<React.PropsWithChildren<SubSectionProps>> = ({
  displayType,
  hasError,
  children,
  label,
  initiallyOpen,
}) => {
  const { formatMessage } = useIntl();

  if (displayType === "expanded") {
    return <>{children}</>;
  }

  return (
    <Collapsible
      label={label || formatMessage({ id: "form.optionalFields" })}
      data-testid="optional-fields"
      showErrorIndicator={hasError}
      type={displayType === "collapsed-footer" ? "footer" : displayType === "collapsed-group" ? "section" : undefined}
      initiallyOpen={initiallyOpen}
      hideWhenEmpty
    >
      {children}
    </Collapsible>
  );
};
