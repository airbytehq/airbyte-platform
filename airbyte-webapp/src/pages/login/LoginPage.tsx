import { SimpleAuthLoginForm } from "components/login/SimpleAuthLoginForm";
import { FlexContainer } from "components/ui/Flex";
import AirbyteLogo from "images/airbyteLogo.svg?react";

import styles from "./LoginPage.module.scss";

export const LoginPage = () => {
  return (
    <main className={styles.loginPage}>
      <div className={styles.loginPage__form}>
        <FlexContainer direction="column" gap="2xl">
          <FlexContainer justifyContent="center">
            <AirbyteLogo className={styles.loginPage__logo} />
          </FlexContainer>
          <SimpleAuthLoginForm />
        </FlexContainer>
      </div>
    </main>
  );
};
