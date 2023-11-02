import classNames from "classnames";
import React from "react";

import styles from "./Icon.module.scss";
import ArrowLeftIcon from "./icons/arrowLeftIcon.svg?react";
import ArrowRightIcon from "./icons/arrowRightIcon.svg?react";
import ArticleIcon from "./icons/articleIcon.svg?react";
import CalendarIcon from "./icons/calendarIcon.svg?react";
import CaretDownIcon from "./icons/caretDownIcon.svg?react";
import CertifiedIcon from "./icons/certifiedIcon.svg?react";
import CheckCircleIcon from "./icons/checkCircleIcon.svg?react";
import CheckIcon from "./icons/checkIcon.svg?react";
import ChevronDownIcon from "./icons/chevronDownIcon.svg?react";
import ChevronLeftIcon from "./icons/chevronLeftIcon.svg?react";
import ChevronRightIcon from "./icons/chevronRightIcon.svg?react";
import ChevronUpIcon from "./icons/chevronUpIcon.svg?react";
import ClockIcon from "./icons/clockIcon.svg?react";
import ClockOutlineIcon from "./icons/clockOutlineIcon.svg?react";
import CopyIcon from "./icons/copyIcon.svg?react";
import CreditsIcon from "./icons/creditsIcon.svg?react";
import CrossIcon from "./icons/crossIcon.svg?react";
import DayIcon from "./icons/dayIcon.svg?react";
import DestinationIcon from "./icons/destinationIcon.svg?react";
import DisabledIcon from "./icons/disabledIcon.svg?react";
import DocsIcon from "./icons/docsIcon.svg?react";
import DownloadIcon from "./icons/downloadIcon.svg?react";
import DuplicateIcon from "./icons/duplicateIcon.svg?react";
import EarthIcon from "./icons/earthIcon.svg?react";
import ErrorIcon from "./icons/errorIcon.svg?react";
import ExpandIcon from "./icons/expandIcon.svg?react";
import EyeIcon from "./icons/eyeIcon.svg?react";
import FileIcon from "./icons/fileIcon.svg?react";
import FlashIcon from "./icons/flashIcon.svg?react";
import FolderIcon from "./icons/folderIcon.svg?react";
import GearIcon from "./icons/gearIcon.svg?react";
import GlobeIcon from "./icons/globeIcon.svg?react";
import GridIcon from "./icons/gridIcon.svg?react";
import ImportIcon from "./icons/importIcon.svg?react";
import InfoIcon from "./icons/infoIcon.svg?react";
import LensIcon from "./icons/lensIcon.svg?react";
import LocationIcon from "./icons/locationIcon.svg?react";
import LockedIcon from "./icons/lockedIcon.svg?react";
import MinusIcon from "./icons/minusIcon.svg?react";
import ModificationIcon from "./icons/modificationIcon.svg?react";
import MoonIcon from "./icons/moonIcon.svg?react";
import MoveHandleIcon from "./icons/moveHandleIcon.svg?react";
import NestedIcon from "./icons/nestedIcon.svg?react";
import NoteIcon from "./icons/noteIcon.svg?react";
import NotificationIcon from "./icons/notificationIcon.svg?react";
import OptionsIcon from "./icons/optionsIcon.svg?react";
import ParametersIcon from "./icons/parametersIcon.svg?react";
import PauseIcon from "./icons/pauseIcon.svg?react";
import PauseOutlineIcon from "./icons/pauseOutlineIcon.svg?react";
import PencilIcon from "./icons/pencilIcon.svg?react";
import PlayIcon from "./icons/playIcon.svg?react";
import PlusIcon from "./icons/plusIcon.svg?react";
import PodcastIcon from "./icons/podcastIcon.svg?react";
import PrefixIcon from "./icons/prefixIcon.svg?react";
import ReduceIcon from "./icons/reduceIcon.svg?react";
import ResetIcon from "./icons/resetIcon.svg?react";
import RotateIcon from "./icons/rotateIcon.svg?react";
import ShareIcon from "./icons/shareIcon.svg?react";
import ShrinkIcon from "./icons/shrinkIcon.svg?react";
import SleepIcon from "./icons/sleepIcon.svg?react";
import SourceIcon from "./icons/sourceIcon.svg?react";
import StarIcon from "./icons/starIcon.svg?react";
import StopIcon from "./icons/stopIcon.svg?react";
import StopOutlineIcon from "./icons/stopOutlineIcon.svg?react";
import SuccessIcon from "./icons/successIcon.svg?react";
import SuccessOutlineIcon from "./icons/successOutlineIcon.svg?react";
import SyncIcon from "./icons/syncIcon.svg?react";
import TargetIcon from "./icons/targetIcon.svg?react";
import TrashIcon from "./icons/trashIcon.svg?react";
import UnlockedIcon from "./icons/unlockedIcon.svg?react";
import UserIcon from "./icons/userIcon.svg?react";
import WarningIcon from "./icons/warningIcon.svg?react";
import WarningOutlineIcon from "./icons/warningOutlineIcon.svg?react";
import { IconColor, IconProps, IconType } from "./types";

const colorMap: Record<IconColor, string> = {
  warning: styles[`icon--warning`],
  success: styles[`icon--success`],
  primary: styles[`icon--primary`],
  disabled: styles[`icon--disabled`],
  error: styles[`icon--error`],
  action: styles[`icon--action`],
  affordance: styles[`icon--affordance`],
};

const sizeMap: Record<NonNullable<IconProps["size"]>, string> = {
  xs: styles.xs,
  sm: styles.sm,
  md: styles.md,
  lg: styles.lg,
  xl: styles.xl,
};

export const Icons: Record<IconType, React.FC<React.SVGProps<SVGSVGElement>>> = {
  arrowLeft: ArrowLeftIcon,
  arrowRight: ArrowRightIcon,
  article: ArticleIcon,
  calendar: CalendarIcon,
  caretDown: CaretDownIcon,
  certified: CertifiedIcon,
  check: CheckIcon,
  checkCircle: CheckCircleIcon,
  chevronDown: ChevronDownIcon,
  chevronLeft: ChevronLeftIcon,
  chevronRight: ChevronRightIcon,
  chevronUp: ChevronUpIcon,
  clock: ClockIcon,
  clockOutline: ClockOutlineIcon,
  copy: CopyIcon,
  credits: CreditsIcon,
  cross: CrossIcon,
  day: DayIcon,
  destination: DestinationIcon,
  disabled: DisabledIcon,
  docs: DocsIcon,
  download: DownloadIcon,
  duplicate: DuplicateIcon,
  earth: EarthIcon,
  error: ErrorIcon,
  eye: EyeIcon,
  expand: ExpandIcon,
  file: FileIcon,
  flash: FlashIcon,
  folder: FolderIcon,
  gear: GearIcon,
  globe: GlobeIcon,
  grid: GridIcon,
  import: ImportIcon,
  info: InfoIcon,
  lens: LensIcon,
  location: LocationIcon,
  locked: LockedIcon,
  minus: MinusIcon,
  modification: ModificationIcon,
  moon: MoonIcon,
  moveHandle: MoveHandleIcon,
  nested: NestedIcon,
  note: NoteIcon,
  notification: NotificationIcon,
  options: OptionsIcon,
  parameters: ParametersIcon,
  pause: PauseIcon,
  pauseOutline: PauseOutlineIcon,
  pencil: PencilIcon,
  play: PlayIcon,
  plus: PlusIcon,
  podcast: PodcastIcon,
  prefix: PrefixIcon,
  reduce: ReduceIcon,
  reset: ResetIcon,
  rotate: RotateIcon,
  share: ShareIcon,
  shrink: ShrinkIcon,
  sleep: SleepIcon,
  source: SourceIcon,
  star: StarIcon,
  stop: StopIcon,
  stopOutline: StopOutlineIcon,
  success: SuccessIcon,
  successOutline: SuccessOutlineIcon,
  sync: SyncIcon,
  target: TargetIcon,
  trash: TrashIcon,
  unlocked: UnlockedIcon,
  user: UserIcon,
  warning: WarningIcon,
  warningOutline: WarningOutlineIcon,
};

export const Icon: React.FC<IconProps> = React.memo(
  ({ type, color, size = "md", withBackground, className, ...props }) => {
    const classes = classNames(
      className,
      styles.icon,
      color ? colorMap[color] : undefined,
      withBackground ? styles["icon--withBackground"] : undefined,
      sizeMap[size]
    );

    return React.createElement(Icons[type], {
      ...props,
      className: classes,
    });
  }
);
Icon.displayName = "Icon";
