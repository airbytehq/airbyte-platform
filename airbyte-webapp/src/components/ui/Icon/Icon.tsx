import classNames from "classnames";
import React from "react";

import styles from "./Icon.module.scss";
import { ReactComponent as ArrowLeftIcon } from "./icons/arrowLeftIcon.svg";
import { ReactComponent as ArrowRightIcon } from "./icons/arrowRightIcon.svg";
import { ReactComponent as ArticleIcon } from "./icons/articleIcon.svg";
import { ReactComponent as CalendarIcon } from "./icons/calendarIcon.svg";
import { ReactComponent as CertifiedIcon } from "./icons/certifiedIcon.svg";
import { ReactComponent as CheckIcon } from "./icons/checkIcon.svg";
import { ReactComponent as ChevronDownIcon } from "./icons/chevronDownIcon.svg";
import { ReactComponent as ChevronLeftIcon } from "./icons/chevronLeftIcon.svg";
import { ReactComponent as ChevronRightIcon } from "./icons/chevronRightIcon.svg";
import { ReactComponent as ChevronUpIcon } from "./icons/chevronUpIcon.svg";
import { ReactComponent as ClockIcon } from "./icons/clockIcon.svg";
import { ReactComponent as ClockOutlineIcon } from "./icons/clockOutlineIcon.svg";
import { ReactComponent as CopyIcon } from "./icons/copyIcon.svg";
import { ReactComponent as CreditsIcon } from "./icons/creditsIcon.svg";
import { ReactComponent as CrossIcon } from "./icons/crossIcon.svg";
import { ReactComponent as DayIcon } from "./icons/dayIcon.svg";
import { ReactComponent as DestinationIcon } from "./icons/destinationIcon.svg";
import { ReactComponent as DisabledIcon } from "./icons/disabledIcon.svg";
import { ReactComponent as DocsIcon } from "./icons/docsIcon.svg";
import { ReactComponent as DownloadIcon } from "./icons/downloadIcon.svg";
import { ReactComponent as DuplicateIcon } from "./icons/duplicateIcon.svg";
import { ReactComponent as EarthIcon } from "./icons/earthIcon.svg";
import { ReactComponent as ErrorIcon } from "./icons/errorIcon.svg";
import { ReactComponent as ExpandIcon } from "./icons/expandIcon.svg";
import { ReactComponent as EyeIcon } from "./icons/eyeIcon.svg";
import { ReactComponent as FileIcon } from "./icons/fileIcon.svg";
import { ReactComponent as FlashIcon } from "./icons/flashIcon.svg";
import { ReactComponent as FolderIcon } from "./icons/folderIcon.svg";
import { ReactComponent as GearIcon } from "./icons/gearIcon.svg";
import { ReactComponent as GlobeIcon } from "./icons/globeIcon.svg";
import { ReactComponent as GridIcon } from "./icons/gridIcon.svg";
import { ReactComponent as ImportIcon } from "./icons/importIcon.svg";
import { ReactComponent as InfoIcon } from "./icons/infoIcon.svg";
import { ReactComponent as LensIcon } from "./icons/lensIcon.svg";
import { ReactComponent as LocationIcon } from "./icons/locationIcon.svg";
import { ReactComponent as LockedIcon } from "./icons/lockedIcon.svg";
import { ReactComponent as MinusIcon } from "./icons/minusIcon.svg";
import { ReactComponent as ModificationIcon } from "./icons/modificationIcon.svg";
import { ReactComponent as MoonIcon } from "./icons/moonIcon.svg";
import { ReactComponent as MoveHandleIcon } from "./icons/moveHandleIcon.svg";
import { ReactComponent as NestedIcon } from "./icons/nestedIcon.svg";
import { ReactComponent as NoteIcon } from "./icons/noteIcon.svg";
import { ReactComponent as NotificationIcon } from "./icons/notificationIcon.svg";
import { ReactComponent as OptionsIcon } from "./icons/optionsIcon.svg";
import { ReactComponent as ParametersIcon } from "./icons/parametersIcon.svg";
import { ReactComponent as PauseIcon } from "./icons/pauseIcon.svg";
import { ReactComponent as PauseOutlineIcon } from "./icons/pauseOutlineIcon.svg";
import { ReactComponent as PencilIcon } from "./icons/pencilIcon.svg";
import { ReactComponent as PlayIcon } from "./icons/playIcon.svg";
import { ReactComponent as PlusIcon } from "./icons/plusIcon.svg";
import { ReactComponent as PodcastIcon } from "./icons/podcastIcon.svg";
import { ReactComponent as PrefixIcon } from "./icons/prefixIcon.svg";
import { ReactComponent as ReduceIcon } from "./icons/reduceIcon.svg";
import { ReactComponent as ResetIcon } from "./icons/resetIcon.svg";
import { ReactComponent as RotateIcon } from "./icons/rotateIcon.svg";
import { ReactComponent as ShareIcon } from "./icons/shareIcon.svg";
import { ReactComponent as ShrinkIcon } from "./icons/shrinkIcon.svg";
import { ReactComponent as SleepIcon } from "./icons/sleepIcon.svg";
import { ReactComponent as SourceIcon } from "./icons/sourceIcon.svg";
import { ReactComponent as StarIcon } from "./icons/starIcon.svg";
import { ReactComponent as StopIcon } from "./icons/stopIcon.svg";
import { ReactComponent as StopOutlineIcon } from "./icons/stopOutlineIcon.svg";
import { ReactComponent as SuccessIcon } from "./icons/successIcon.svg";
import { ReactComponent as SuccessOutlineIcon } from "./icons/successOutlineIcon.svg";
import { ReactComponent as SyncIcon } from "./icons/syncIcon.svg";
import { ReactComponent as TargetIcon } from "./icons/targetIcon.svg";
import { ReactComponent as TrashIcon } from "./icons/trashIcon.svg";
import { ReactComponent as UnlockedIcon } from "./icons/unlockedIcon.svg";
import { ReactComponent as UserIcon } from "./icons/userIcon.svg";
import { ReactComponent as WarningIcon } from "./icons/warningIcon.svg";
import { ReactComponent as WarningOutlineIcon } from "./icons/warningOutlineIcon.svg";
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

const Icons: Record<IconType, React.FC<React.SVGProps<SVGSVGElement>>> = {
  arrowLeft: ArrowLeftIcon,
  arrowRight: ArrowRightIcon,
  article: ArticleIcon,
  calendar: CalendarIcon,
  certified: CertifiedIcon,
  check: CheckIcon,
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
