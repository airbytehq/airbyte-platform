import { useState } from "react";
import { useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { HttpProblem, useCreateDomainVerification } from "core/api";
import { DomainVerificationResponse } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./DomainVerification.module.scss";

interface AddDomainModalProps {
  onClose: () => void;
  existingDomain?: DomainVerificationResponse;
}

interface DomainFormValues {
  domain: string;
}

type ModalStep = "enterDomain" | "showInstructions";

export const DomainVerificationModal: React.FC<AddDomainModalProps> = ({ onClose, existingDomain }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const formatError = useFormatError();
  const { mutateAsync: createDomain, isLoading } = useCreateDomainVerification();

  const [currentStep, setCurrentStep] = useState<ModalStep>(existingDomain ? "showInstructions" : "enterDomain");
  const [domainResponse, setDomainResponse] = useState<DomainVerificationResponse | null>(existingDomain ?? null);

  const {
    register,
    handleSubmit,
    formState: { errors, isValid },
  } = useForm<DomainFormValues>({
    mode: "onBlur",
    reValidateMode: "onChange",
    defaultValues: {
      domain: "",
    },
  });

  /**
   * Extracts a clean domain name from various user input formats.
   *
   * Handles common paste scenarios from browser address bars:
   * - "https://example.com/" → "example.com"
   * - "https://example.com/login" → "example.com"
   * - "https://example.com:8443" → "example.com"
   * - "http://example.com:3000/path" → "example.com"
   *
   * @param value - Raw user input that may include scheme, path, port, or trailing slash
   * @returns Clean domain name suitable for DNS verification
   */
  const cleanDomainInput = (value: string) =>
    value
      .trim()
      .replace(/^https?:\/\//i, "") // Remove http:// or https:// (case-insensitive)
      .split("/")[0] // Remove any path segments or trailing slashes
      .replace(/:\d+$/, ""); // Remove port numbers

  const validateDomain = (value: string) => {
    const cleanDomain = cleanDomainInput(value);
    // - Each segment must start and end with alphanumeric
    // - Segments can contain hyphens in the middle (max 63 chars)
    // - Must have at least one dot
    // - TLD must be at least 2 characters
    const domainRegex = /^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$/;
    if (!domainRegex.test(cleanDomain)) {
      return formatMessage({ id: "settings.organizationSettings.domainVerification.invalidDomain" });
    }

    return true;
  };

  const onAddDomain = async (values: DomainFormValues) => {
    try {
      const cleanDomain = cleanDomainInput(values.domain);
      const response = await createDomain(cleanDomain);

      setDomainResponse(response);
      setCurrentStep("showInstructions");

      registerNotification({
        id: "domain-verification-created",
        text: formatMessage({ id: "settings.organizationSettings.domainVerification.created" }),
        type: "success",
      });
    } catch (error) {
      const errorMessage = HttpProblem.isInstanceOf(error)
        ? formatError(error)
        : formatMessage({ id: "settings.organizationSettings.domainVerification.createError" });

      registerNotification({
        id: "domain-verification-create-error",
        text: errorMessage,
        type: "error",
      });
    }
  };

  const modalTitle = existingDomain
    ? formatMessage({ id: "settings.organizationSettings.domainVerification.viewDnsInfo" })
    : formatMessage({ id: "settings.organizationSettings.domainVerification.addDomain" });

  return (
    <Modal size="md" title={modalTitle} onCancel={currentStep === "enterDomain" ? onClose : undefined}>
      <form onSubmit={handleSubmit(onAddDomain)}>
        <ModalBody>
          {currentStep === "enterDomain" && (
            <FlexContainer direction="column" gap="lg">
              <Text>
                <FormattedMessage id="settings.organizationSettings.domainVerification.addDomainDescription" />
              </Text>

              <FlexContainer direction="column" gap="sm">
                <Text bold size="sm">
                  {formatMessage({ id: "settings.organizationSettings.domainVerification.domainName" })}
                </Text>
                <Input
                  {...register("domain", {
                    required: formatMessage({ id: "form.empty.error" }),
                    validate: validateDomain,
                  })}
                  placeholder="example.com"
                  error={!!errors.domain}
                />
                {errors.domain && (
                  <Text size="sm" color="red">
                    {errors.domain.message}
                  </Text>
                )}
              </FlexContainer>
            </FlexContainer>
          )}

          {currentStep === "showInstructions" && domainResponse && (
            <FlexContainer direction="column" gap="lg">
              <FlexContainer direction="column" gap="sm">
                <Text bold size="sm">
                  {formatMessage({ id: "settings.organizationSettings.domainVerification.domainName" })}
                </Text>
                <Input value={domainResponse.domain} disabled />
              </FlexContainer>

              <Text bold>
                <FormattedMessage id="settings.organizationSettings.domainVerification.dnsInstructions" />
              </Text>

              <Text>
                <FormattedMessage id="settings.organizationSettings.domainVerification.dnsInstructionsDescription" />
              </Text>

              <div className={styles.dnsInstructionsBox}>
                <FlexContainer direction="column" gap="md">
                  <FlexContainer direction="column" gap="sm">
                    <Text size="sm" bold>
                      <FormattedMessage id="settings.organizationSettings.domainVerification.recordType" />
                    </Text>
                    <Text>TXT</Text>
                  </FlexContainer>

                  <FlexContainer direction="column" gap="sm">
                    <FlexContainer justifyContent="space-between" alignItems="center">
                      <Text size="sm" bold>
                        <FormattedMessage id="settings.organizationSettings.domainVerification.recordName" />
                      </Text>
                      {domainResponse.dnsRecordName && (
                        <CopyButton content={domainResponse.dnsRecordName} variant="secondary" />
                      )}
                    </FlexContainer>
                    <Text className={styles.monospaceText}>{domainResponse.dnsRecordName || "N/A"}</Text>
                  </FlexContainer>

                  <FlexContainer direction="column" gap="sm">
                    <FlexContainer justifyContent="space-between" alignItems="center">
                      <Text size="sm" bold>
                        <FormattedMessage id="settings.organizationSettings.domainVerification.recordValue" />
                      </Text>
                      {domainResponse.dnsRecordValue && (
                        <CopyButton content={domainResponse.dnsRecordValue} variant="secondary" />
                      )}
                    </FlexContainer>
                    <Text className={styles.monospaceText}>{domainResponse.dnsRecordValue || "N/A"}</Text>
                  </FlexContainer>
                </FlexContainer>
              </div>

              <Text size="sm" color="grey" italicized>
                <FormattedMessage id="settings.organizationSettings.domainVerification.verificationNote" />
              </Text>
            </FlexContainer>
          )}
        </ModalBody>

        <ModalFooter>
          <FlexContainer justifyContent="flex-end" gap="sm">
            {currentStep === "enterDomain" && (
              <>
                <Button type="button" variant="secondary" onClick={onClose}>
                  <FormattedMessage id="form.cancel" />
                </Button>
                <Button type="submit" disabled={!isValid || isLoading} isLoading={isLoading}>
                  <FormattedMessage id="settings.organizationSettings.domainVerification.addDomain" />
                </Button>
              </>
            )}

            {currentStep === "showInstructions" && (
              <Button type="button" onClick={onClose}>
                <FormattedMessage id="form.done" />
              </Button>
            )}
          </FlexContainer>
        </ModalFooter>
      </form>
    </Modal>
  );
};
