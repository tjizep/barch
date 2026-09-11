import type { DefineComponent } from "vue";
import type { IMenuOption } from "@svar-ui/vue-menu";
import { Tooltip as BaseTooltip } from "@svar-ui/vue-core";

import type {
	TMethodsConfig,
	IApi,
	IConfig,
	TContextMenuType,
	IExtraInfo,
	IParsedEntity,
} from "@svar-ui/filemanager-store";

export * from "@svar-ui/filemanager-store";

export interface IFileMenuOption extends IMenuOption {
	hotkey: string;
}

export type FilePreview = IParsedEntity & {
	type: "file" | "folder" | "search" | "multiple" | "none";
};

/* get component events from store actions*/
type RemoveHyphen<S extends string> = S extends `${infer Head}-${infer Tail}`
	? `${Head}${RemoveHyphen<Tail>}`
	: S;

type EventName<K extends string> = `on${RemoveHyphen<K>}`;

export type FilemanagerActions<TMethodsConfig extends Record<string, any>> = {
	[K in keyof TMethodsConfig as EventName<K & string>]?: (
		ev: TMethodsConfig[K]
	) => void;
} & {
	[key: `on${string}`]: (ev?: any) => void;
};

export declare const Filemanager: DefineComponent<
	{
		readonly?: boolean;
		menuOptions?: (
			mode: TContextMenuType,
			item?: IParsedEntity
		) => IFileMenuOption[];
		extraInfo?: (
			file: IParsedEntity
		) => Promise<IExtraInfo> | IExtraInfo | null;
		icons?: (file: IParsedEntity, size: "big" | "small") => string;
		previews?: (
			file: FilePreview,
			width: number,
			height: number
		) => string | null;
		init?: (api: IApi) => void;
	} & IConfig &
		FilemanagerActions<TMethodsConfig>
>;

type TooltipContentData = {
	file: IParsedEntity;
};

export declare const Tooltip: DefineComponent<
	Omit<InstanceType<typeof BaseTooltip>["$props"], "content"> & {
		api?: IApi;
		overflow?: boolean;
		content?: DefineComponent<{
			api: IApi;
			data: TooltipContentData;
		}>;
	}
>;

export declare const Willow: DefineComponent<{
	fonts?: boolean;
}>;

export declare const WillowDark: DefineComponent<{
	fonts?: boolean;
}>;
