<script setup>
defineOptions({ name: "FilemanagerLayout" });

import { ref, computed, inject, onMounted, onUnmounted, nextTick } from "vue";
import { clickOutside, locateID, setID } from "@svar-ui/lib-dom";
import { ActionMenu, ContextMenu } from "@svar-ui/vue-menu";
import { Uploader } from "@svar-ui/vue-uploader";
import { hotkeys } from "@svar-ui/filemanager-store";
import { subscribe, asDirective } from "@svar-ui/lib-vue";

import SearchView from "./SearchView.vue";
import Info from "./Info.vue";
import Panels from "./Panels.vue";
import Sidebar from "./Sidebar.vue";
import Toolbar from "./Toolbar.vue";
import TableView from "./Table/View.vue";
import CardsView from "./Cards/View.vue";

const vClickOutside = asDirective(clickOutside);
const vHotkeys = asDirective(hotkeys);

const props = defineProps({
	readonly: { type: Boolean, default: false },
	menuOptions: { type: Function },
	extraInfo: { type: Function },
});

const sidebarWidth = ref(undefined);
const narrowMode = ref(undefined);
const showSidebar = ref(false);

const rootRef = ref(null);
const sidebarRef = ref(null);
let resizeObserver;
let sidebarResizeObserver;

onMounted(() => {
	if (rootRef.value) {
		resizeObserver = new ResizeObserver(entries => {
			narrowMode.value =
				entries[0].borderBoxSize[0].inlineSize < 768;
		});
		resizeObserver.observe(rootRef.value);
	}
});

onMounted(() => {
	if (sidebarRef.value) {
		sidebarWidth.value = sidebarRef.value.clientWidth;
		sidebarResizeObserver = new ResizeObserver(entries => {
			sidebarWidth.value =
				entries[0].borderBoxSize[0].inlineSize;
		});
		sidebarResizeObserver.observe(sidebarRef.value);
	}
});

onUnmounted(() => {
	resizeObserver?.disconnect();
	sidebarResizeObserver?.disconnect();
});

function toggleSidebar() {
	showSidebar.value = !showSidebar.value;
}

function hideSidebar(ev) {
	if (narrowMode.value && locateID(ev) !== "toggle-tree") {
		showSidebar.value = false;
	}
}

const api = inject("filemanager-store");
const { showPrompt, showConfirm } = inject("filemanager-modals");
const _ = inject("wx-i18n").getGroup("filemanager");

const {
	mode: rMode,
	preview: rPreview,
	activePanel: rActivePanel,
	panels: rPanels,
} = api.getReactiveState();

const mode = subscribe(rMode);
const preview = subscribe(rPreview);
const activePanel = subscribe(rActivePanel);
const panels = subscribe(rPanels, true);

const selected = computed(() => panels.value[activePanel.value].selected);
const path = computed(() => panels.value[activePanel.value].path);
let contextMenuOptions = ref([]);

const previewOption = {
	icon: "wxi-eye",
	text: "Preview",
	id: "preview",
};

function getReadonlyMenu(type) {
	let options = narrowMode.value ? [previewOption] : [];
	if (type === "file")
		options = [
			...options,
			...props.menuOptions().filter(o => o.id === "download"),
		];
	return options;
}

function resolveContext(id, e) {
	const item = id ? api.getFile(id) : null;
	const inTree = e.target.closest(".tree-item.folder");

	let options;

	if (props.readonly) {
		options = getReadonlyMenu(item?.type);
	} else {
		switch (id) {
			case "body":
				options = props.menuOptions(id);
				break;
			default:
				if (item) {
					if (!inTree && selected.value.length > 1) {
						options = narrowMode.value
							? [previewOption, ...props.menuOptions("multiselect")]
							: props.menuOptions("multiselect");
					} else if (id === "/") {
						options = props
							.menuOptions("folder", item)
							.filter(o => o.id === "paste");
					} else {
						const mOptions = props.menuOptions(item.type, item);
						if (mOptions) {
							options = narrowMode.value
								? [previewOption, ...mOptions]
								: mOptions;
						}
					}
				}
		}
		if (mode.value === "search") {
			options = options?.filter(o => o.id !== "paste");
		}
	}

	if (
		item?.id &&
		(!selected.value.length ||
			!selected.value.some(i => i === item.id)) &&
		!inTree
	) {
		api.exec("select-file", { id: item.id });
	}

	if (options?.length) {
		options.forEach(o => {
			if (inTree) o.hotkey = "";
			if (o.text) o.text = _(o.text);
			if (o.hotkey) o.subtext = o.hotkey;
		});
		contextMenuOptions.value = options;
		return item || {};
	}
}

let copy = null;
function handleMenu(e) {
	const { action, context } = e;
	if (action) {
		performAction(action.id, context, !action.hotkey);
	}
}

function performAction(action, context, inTree) {
	const ids = inTree ? [context.id] : selected.value;
	switch (action) {
		case "download":
			api.exec("download-file", { id: context.id });
			break;
		case "copy":
		case "move":
			copy = {
				action,
				ids: ids,
			};
			break;
		case "paste":
			if (copy) {
				api.exec(
					copy.action === "copy" ? "copy-files" : "move-files",
					{
						ids: copy.ids,
						target:
							context?.type === "folder"
								? context.id
								: path.value,
					}
				);
			}
			break;
		case "rename":
			showPrompt({ item: context });
			break;
		case "delete":
			showConfirm({ selected: ids });
			break;
		case "preview":
			api.exec("show-preview", { mode: !preview.value });
			break;
	}
}

function getPanel() {
	return panels.value[activePanel.value];
}

async function handleUpload(f) {
	//if active panel was changed before upload, wait until for it
	await nextTick();
	const { name, size } = f.file;

	api.exec("create-file", {
		file: {
			name,
			size,
			date: new Date(),
			file: f.file,
		},
		parent: path.value,
	});
}

const hotkeysConfig = computed(() => ({
	api,
	menuOptions: props.readonly ? getReadonlyMenu : props.menuOptions,
	performAction,
	getPanel,
}));
</script>

<template>
	<div
		ref="rootRef"
		class="wx-filemanager wx-flex"
		v-hotkeys="hotkeysConfig"
	>
		<template v-if="narrowMode && preview">
			<div class="wx-info-narrow">
				<Info :narrow-mode="narrowMode" :extra-info="extraInfo" />
			</div>
		</template>
		<template v-else>
			<Toolbar
				:narrow-mode="narrowMode"
				:onshowtree="toggleSidebar"
			/>
			<ContextMenu
				data-key="id"
				at="point"
				:options="contextMenuOptions"
				:resolver="resolveContext"
				:onclick="handleMenu"
			>
				<ActionMenu
					data-key="actionId"
					:options="contextMenuOptions"
					:resolver="resolveContext"
					:onclick="handleMenu"
				>
					<Uploader :api-only="true" :upload-u-r-l="handleUpload">
						<div class="wx-content-wrapper wx-flex">
							<template
								v-if="
									mode !== 'panels' &&
									mode !== 'search'
								"
							>
								<div
									ref="sidebarRef"
									v-click-outside="hideSidebar"
									:class="[
										'wx-sidebar',
										{
											'wx-sidebar-narrow':
												narrowMode,
											'wx-sidebar-visible':
												showSidebar,
										},
									]"
								>
									<Sidebar
										:readonly="readonly"
										:menu-options="menuOptions"
									/>
								</div>
								<div
									class="wx-content"
									:style="`width: calc(100% - ${sidebarWidth}px - 10px)`"
								>
									<div
										:class="
											preview
												? 'wx-content-item'
												: 'wx-content-item-fit'
										"
										:data-id="setID('body')"
									>
										<TableView
											v-if="mode === 'table'"
											:panel="activePanel"
										/>
										<CardsView v-else />
									</div>
									<div v-if="preview" class="wx-info">
										<Info :extra-info="extraInfo" />
									</div>
								</div>
							</template>
							<template v-else>
								<div
									:class="
										preview
											? 'wx-content-item'
											: 'wx-content-item-fit'
									"
								>
									<Panels v-if="mode === 'panels'" />
									<SearchView v-else />
								</div>
								<div v-if="preview" class="wx-info">
									<Info :extra-info="extraInfo" />
								</div>
							</template>
						</div>
					</Uploader>
				</ActionMenu>
			</ContextMenu>
		</template>
	</div>
</template>

<style scoped>
.wx-flex {
	display: flex;
	width: 100%;
}
.wx-filemanager {
	max-width: 100vw;
	max-height: 100vh;
	overflow: hidden;
	background-color: var(--wx-fm-background);
	flex-direction: column;
	height: 100%;
}
.wx-content {
	flex-grow: 1;
	display: flex;
	flex-shrink: 0;
}
.wx-content-item-fit {
	width: 100%;
	padding: 0 10px 10px;
}
.wx-content-item {
	width: 67%;
	padding: 0 10px 10px;
}
.wx-content-wrapper {
	margin-top: 10px;
	max-width: 100%;
	max-height: 100%;
	position: relative;
}
.wx-info {
	width: 38%;
}
.wx-sidebar {
	flex: 0 0 auto;
	width: 238px;
	padding: 0 10px 10px;
	height: 100%;
}
.wx-sidebar-narrow {
	position: absolute !important;
	z-index: 5;
	left: -300px;
	transition-duration: 0.6s;
}
.wx-sidebar-visible {
	left: 0;
}
.wx-info-narrow {
	width: 100%;
	height: 100%;
	padding-top: 10px;
}
.wx-filemanager > :deep(div[data-menu-ignore="true"]) {
	height: 100%;
	width: 100%;
}
.wx-filemanager > :deep(span[data-menu-ignore="true"]),
.wx-filemanager
	> :deep(span[data-menu-ignore="true"] > span[data-menu-ignore="true"]) {
	min-height: 0;
	height: 100%;
	width: 100%;
	display: flex;
}
</style>
