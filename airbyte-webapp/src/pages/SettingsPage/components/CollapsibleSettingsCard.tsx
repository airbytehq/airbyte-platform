import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import { motion, AnimatePresence } from "framer-motion";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import styles from "./CollapsibleSettingsCard.module.scss";

interface CollapsibleSettingsCardProps {
  label: string;
  status?: React.ReactNode;
  docsLink?: string;
  docsLinkLabel?: React.ReactNode;
  defaultOpen?: boolean;
  children?: React.ReactNode;
}

export const CollapsibleSettingsCard: React.FC<CollapsibleSettingsCardProps> = ({
  label,
  status,
  docsLink,
  docsLinkLabel,
  defaultOpen,
  children,
}) => {
  return (
    <div className={styles.card}>
      <Disclosure defaultOpen={defaultOpen}>
        {({ open }) => (
          <>
            <FlexContainer gap="md" alignItems="center">
              <DisclosureButton className={styles["card__header-toggle"]}>
                <FlexContainer gap="md" alignItems="center" className={styles.card__header}>
                  <motion.div animate={{ rotate: open ? 90 : 0 }} transition={{ duration: 0.2, ease: "easeInOut" }}>
                    <Icon type="chevronRight" />
                  </motion.div>
                  <Text bold>{label}</Text>
                  {status}
                </FlexContainer>
              </DisclosureButton>
              {docsLink && (
                <ExternalLink href={docsLink} className={styles["card__header-link"]}>
                  <Button
                    type="button"
                    variant="link"
                    icon="arrowRight"
                    size="sm"
                    iconPosition="right"
                    iconSize="sm"
                    className={styles["card__header-button"]}
                  >
                    {docsLinkLabel}
                  </Button>
                </ExternalLink>
              )}
            </FlexContainer>

            <AnimatePresence initial={false}>
              {open && (
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.2, ease: "easeInOut" }}
                >
                  <DisclosurePanel static>
                    <Box mt="lg">{children}</Box>
                  </DisclosurePanel>
                </motion.div>
              )}
            </AnimatePresence>
          </>
        )}
      </Disclosure>
    </div>
  );
};
