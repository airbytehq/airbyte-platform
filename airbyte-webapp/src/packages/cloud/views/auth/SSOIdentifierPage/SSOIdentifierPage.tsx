import { ReactNode } from "react";
import { useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation } from "react-router-dom";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionHandler } from "components/forms/Form";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useAuthService } from "core/services/auth";
import { links } from "core/utils/links";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { SSO_LOGIN_REQUIRED_STATE } from "packages/cloud/services/auth/CloudAuthService";

import styles from "./SSOIdentifierPage.module.scss";

const schema = z.object({
  companyIdentifier: z.string().trim().nonempty("form.empty.error"),
});

type CompanyIdentifierValues = z.infer<typeof schema>;

export const SSOIdentifierPage = () => {
  const [lastSsoCompanyIdentifier, setLastSsoCompanyIdentifier] = useLocalStorage(
    "airbyte_last-sso-company-identifier",
    ""
  );
  const { changeRealmAndRedirectToSignin } = useAuthService();
  const { formatMessage } = useIntl();
  const { state } = useLocation();

  if (!changeRealmAndRedirectToSignin) {
    throw new Error("Rendered SSOIdentifierPage while AuthService does not provide changeRealmAndRedirectToSignin");
  }

  const handleSubmit: FormSubmissionHandler<CompanyIdentifierValues> = async ({ companyIdentifier }, methods) => {
    try {
      setLastSsoCompanyIdentifier(companyIdentifier);
      return await changeRealmAndRedirectToSignin(companyIdentifier);
    } catch (e) {
      setLastSsoCompanyIdentifier("");
      methods.setError("companyIdentifier", { message: "login.sso.invalidCompanyIdentifier" });
      return Promise.reject();
    }
  };

  return (
    <main className={styles.ssoIdentifierPage}>
      <Form<CompanyIdentifierValues>
        onSubmit={handleSubmit}
        defaultValues={{ companyIdentifier: lastSsoCompanyIdentifier }}
        zodSchema={schema}
      >
        <Box mb="md">
          <Heading as="h1" size="xl" color="blue">
            <FormattedMessage id="login.sso.title" />
          </Heading>
        </Box>
        {state?.[SSO_LOGIN_REQUIRED_STATE] && (
          <Box mb="lg">
            <Message type="warning" text={formatMessage({ id: "login.sso.ssoLoginRequired" })} />
          </Box>
        )}
        <Box mb="lg">
          <Text>
            <FormattedMessage id="login.sso.description" />
          </Text>
        </Box>
        <FormControl<CompanyIdentifierValues>
          fieldType="input"
          name="companyIdentifier"
          placeholder={formatMessage({ id: "login.sso.companyIdentifier.placeholder" })}
          label={formatMessage({ id: "login.sso.companyIdentifier.label" })}
        />
        <FormSubmissionButton />
        <Box my="xl">
          <BookmarkableUrl />
        </Box>
        <FlexContainer gap="md">
          <FlexItem grow={false}>
            <Icon type="comments" />
          </FlexItem>
          <Text>
            <FormattedMessage
              id="login.sso.getSsoLogin"
              values={{ a: (content: ReactNode) => <Link to={links.contactSales}>{content}</Link> }}
            />
          </Text>
        </FlexContainer>
      </Form>
    </main>
  );
};

const FormSubmissionButton = () => {
  const { isSubmitting } = useFormState<CompanyIdentifierValues>();

  return (
    <FlexContainer justifyContent="space-between" alignItems="center">
      <Link to={CloudRoutes.Login}>
        <Text color="grey">
          <FormattedMessage id="login.backLogin" />
        </Text>
      </Link>
      <Button type="submit" isLoading={isSubmitting}>
        <FormattedMessage id="login.sso.continueWithSSO" />
      </Button>
    </FlexContainer>
  );
};

const BookmarkableUrl = () => {
  const fieldValue = useWatch<CompanyIdentifierValues>({ name: "companyIdentifier" });
  const { formatMessage } = useIntl();
  const placeholder = formatMessage({ id: "login.sso.companyIdentifier.placeholder" });
  const companyHandle = fieldValue.trim() || placeholder;

  return (
    <FlexContainer gap="md">
      <FlexItem grow={false}>
        <Icon type="lightbulb" />
      </FlexItem>
      <Text>
        <FormattedMessage
          id="login.sso.quickLink"
          values={{
            a: (content: ReactNode) => <Link to={`${CloudRoutes.Sso}/${companyHandle}`}>{content}</Link>,
            companyIdentifier: companyHandle,
          }}
        />
      </Text>
    </FlexContainer>
  );
};
