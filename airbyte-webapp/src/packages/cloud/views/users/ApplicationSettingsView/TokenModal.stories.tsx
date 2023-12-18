import { StoryObj } from "@storybook/react";
import { FormattedMessage } from "react-intl";

import { Modal } from "components/ui/Modal";

import { TokenModalBody } from "./TokenModal";

interface TokenWrapperProps {
  tokenSize: "small" | "large";
}

const TokenWrapper: React.FC<TokenWrapperProps> = ({ tokenSize, ...args }) => {
  const longerToken =
    "N3bvmgb-wsZ5SJKk?g-t7asdfasdfasgrwaerwetwetq345q325i8nq3p48yq3p98u5[0c2q956c-qOIU87EK8wkwjaraewtki7gaerNvnDUOa6j-!ufL4NNSi=R8HIzjwjKKRbSU8!?M1ipLmrQs7fnoP!qFMLUIhJXTY4Tri=Sy51=WV=LwF8hpLw=Vw/R8k1uOX7cU5-WUw4NS2tYoCA5GNXYZ1joTy2Es2Vi1Ni=CTTILX?cauM0y8eQgpwKmy1orjNvSZlFEtX6mkQjHKNRtOOSm!HHhJHeRwmydAwSA94azP56Sp99L9vXaLGPEpJYMPblYiG195-a24N3bvmgb-wsZ5SJKk?g-t7NvnDUOa6j-!ufL4NNSi=R8HIzjwjKKRbSU8!?M1ipLmrQs7fnoP!qFMLUIhJXTY4Tri=Sy51=WV=LwF8hpLw=Vw/R8k1uOX7cU5-WUw4NS2tYoCA5GNXYZ1joTy2Es2Vi1Ni=CTTILX?cauM0y8eQgpwKmy1orjNvSZlFEtX6mkQjHKNRtOOSm!HHhJHeRwmydAwSA94azP56Sp99L9vXaLGPEpJYMPblYiG195-a24";

  const shorterToken =
    "N3bvmgb-wsZ5SJKk?g-t7NvnDUOa6j-!ufL4NNSi=R8HIzjwjKKRbSU8!?M1ipLmrQs7fnoP!qFMLUIhJXTY4Tri=Sy51=WV=LwF8hpLw=Vw/R8k1uOX7cU5-WUw4NS2tYoCA5GNXYZ1joTy2Es2Vi1Ni=CTTILX?cauM0y8eQgpwKmy1orjNvSZlFEtX6mkQjHKNRtOOSm!HHhJHeRwmydAwSA94azP56Sp99L9vXaLGPEpJYMPblYiG195-a24";

  return (
    <Modal size="md" title={<FormattedMessage id="settings.applications.token.new" />}>
      <TokenModalBody token={tokenSize === "small" ? shorterToken : longerToken} {...args} />
    </Modal>
  );
};

export default {
  title: "Settings/TokenModal",
  component: TokenModalBody,
  argTypes: {
    tokenSize: {
      options: ["small", "large"],
      control: {
        type: "radio",
      },
    },
  },
} as StoryObj<typeof TokenModalBody>;

export const Default: StoryObj<typeof TokenWrapper> = {
  args: {
    tokenSize: "large",
  },
  render: (args) => <TokenWrapper {...args} />,
};
