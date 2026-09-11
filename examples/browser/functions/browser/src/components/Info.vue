<script setup>
defineOptions({ name: "FilemanagerInfo" });

import { ref, computed, inject, watchEffect } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import { dateToString } from "@svar-ui/lib-dom";
import { formatSize } from "@svar-ui/filemanager-store";
import Icon from "./ui/Icon.vue";

const props = defineProps({
	narrowMode: { type: Boolean },
	extraInfo: { type: Function },
});

const api = inject("filemanager-store");
const { preview, icon } = api.templates;

const {
	panels,
	activePanel,
	preview: rPreview,
	search,
} = api.getReactiveState();

const sPanels = subscribe(panels, true);
const sActivePanel = subscribe(activePanel);
const sPreview = subscribe(rPreview);
const sSearch = subscribe(search);

const locale = inject("wx-i18n");
const _ = locale.getGroup("filemanager");
const format = dateToString("%D, %d %F %Y, %H:%i", locale.getRaw().calendar);

const found = computed(
	() =>
		sSearch.value && !sPanels.value[sActivePanel.value]._selected.length
);
const items = computed(() =>
	found.value
		? sPanels.value[sActivePanel.value]._files
		: sPanels.value[sActivePanel.value]._selected
);

function downloadFile() {
	api.exec("download-file", {
		id: items.value[0].id,
	});
}

function getTotalCount(folders, files) {
	return (
		(folders
			? `${folders} ${_(folders > 1 ? "folders" : "folder")} `
			: "") +
		(files ? `${files} ${_(files > 1 ? "files" : "file")}` : "")
	);
}

function closePreview() {
	api.exec("show-preview", { mode: !sPreview.value });
}

function getItemType(item) {
	return item.type === "folder" ? _("Folder") : item.ext || _("Unknown file");
}

const basic = computed(() => {
	let name,
		item,
		previewSrc,
		iconSrc,
		showDownloadIcon,
		type,
		totalCount,
		totalSize;

	if (items.value.length === 1 && !found.value) {
		item = items.value[0];
		name = item.name;
		const prev = preview(item, 400, 500);
		if (prev) {
			previewSrc = prev;
			iconSrc = "";
		} else {
			iconSrc = icon(item, "big");
			previewSrc = "";
		}
		showDownloadIcon = item.type !== "folder";
	} else {
		item = showDownloadIcon = null;
		name = "";
		if (items.value.length) {
			let sum = 0;
			let folders = 0;
			let files = 0;
			let extArr = [];
			let incorrectSize;
			items.value.forEach(item => {
				if (typeof item.size === "undefined") {
					incorrectSize = true;
					sum = undefined;
				}

				if (!incorrectSize) {
					sum += item.size;
				}

				extArr.push(getItemType(item));
				item.type === "folder" ? folders++ : files++;
			});

			const singleExt = new Set(extArr);
			type = singleExt.size > 1 ? _("multiple") : [...singleExt][0];
			totalCount = getTotalCount(folders, files);
			totalSize = sum;
			if (!found.value) name = _("Multiple files");
		}
		iconSrc = icon(
			{
				type: found.value
					? "search"
					: items.value.length
						? "multiple"
						: "none",
			},
			"big"
		);
		previewSrc = "";
	}

	return {
		name,
		item,
		previewSrc,
		iconSrc,
		showDownloadIcon,
		type,
		totalCount,
		totalSize,
	};
});

async function getExtraInfo(item) {
	if (!props.extraInfo || !item) return null;
	try {
		return await props.extraInfo(item);
	} catch (e) {
		console.log(e);
		return null;
	}
}

const info = ref(null);
watchEffect(async () => {
	info.value = await getExtraInfo(basic.value.item);
});
</script>

<template>
	<div class="wx-wrapper">
		<template v-if="items.length">
			<div class="wx-preview">
				<div class="wx-toolbar">
					<div class="wx-name">{{ basic.name }}</div>
					<div class="wx-icons">
						<Icon
							v-if="basic.showDownloadIcon"
							name="download"
							:onclick="downloadFile"
						/>
						<Icon
							v-if="narrowMode"
							name="close"
							:onclick="closePreview"
						/>
					</div>
				</div>
				<div v-if="basic.previewSrc" class="wx-img-wrapper">
					<img
						:src="basic.previewSrc"
						:alt="_('A miniature file preview')"
					/>
				</div>
				<div v-else-if="basic.iconSrc" class="wx-icon-wrapper">
					<img
						:src="basic.iconSrc"
						:alt="_('A miniature file preview')"
					/>
				</div>
				<div v-else class="wx-icon-wrapper">
					<i
						v-if="basic.item"
						:class="'wx-major-icon wxi-' + basic.item.type"
					></i>
					<i
						v-else
						:class="
							'wx-major-icon wxi-' +
							(found ? 'search' : 'file-multiple-outline')
						"
					></i>
				</div>
			</div>
			<div class="wx-info-panel">
				<div class="wx-title">
					{{ found ? _("Found") : _("Information") }}
				</div>
				<div class="wx-list">
					<template v-if="basic.item">
						<span class="wx-name">{{ _("Type") }}</span>
						<span class="wx-value">{{
							getItemType(basic.item)
						}}</span>
						<template
							v-if="typeof basic.item.size !== 'undefined'"
						>
							<span class="wx-name">{{ _("Size") }}</span>
							<span class="wx-value">{{
								formatSize(basic.item.size)
							}}</span>
						</template>
						<span class="wx-name">{{ _("Date") }}</span>
						<span class="wx-value">{{ format(basic.item.date) }}
						</span>
					</template>
					<template v-else>
						<span class="wx-name">{{ _("Count") }}</span>
						<span class="wx-value">{{ basic.totalCount }}</span>
						<span class="wx-name">{{ _("Type") }}</span>
						<span class="wx-value">{{ basic.type }}</span>
						<template
							v-if="typeof basic.totalSize !== 'undefined'"
						>
							<span class="wx-name">{{ _("Size") }}</span>
							<span class="wx-value">{{
								formatSize(basic.totalSize)
							}}</span>
						</template>
					</template>
					<template v-if="info">
						<template
							v-for="[entryName, entryValue] in Object.entries(
								info
							)"
							:key="entryName"
						>
							<span class="wx-name">{{ entryName }}</span>
							<span class="wx-value">{{ entryValue }}</span>
						</template>
					</template>
				</div>
			</div>
		</template>
		<div v-else class="wx-no-info-panel">
			<div class="wx-toolbar">
				<div class="wx-name">{{ basic.name }}</div>
				<div class="wx-icons">
					<Icon
						v-if="narrowMode"
						name="close"
						:onclick="closePreview"
					/>
				</div>
			</div>
			<div class="wx-no-info-wrapper">
				<div class="wx-no-info">
					<div v-if="basic.previewSrc" class="wx-img-wrapper">
						<img
							:src="basic.previewSrc"
							:alt="_('A miniature file preview')"
						/>
					</div>
					<div
						v-else-if="basic.iconSrc"
						class="wx-icon-wrapper"
					>
						<img
							:src="basic.iconSrc"
							:alt="_('A miniature file preview')"
						/>
					</div>
					<div v-else class="wx-icon-wrapper">
						<i
							:class="
								'wx-major-icon wxi-' +
								(found
									? 'search'
									: 'message-question-outline')
							"
						></i>
					</div>
					<span class="wx-text">{{
						found
							? ""
							: _("Select file or folder to view details")
					}}</span>
				</div>
			</div>
		</div>
	</div>
</template>

<style scoped>
.wx-wrapper {
	height: 100%;
	width: 100%;
	cursor: default;
	padding: 0 10px 10px;
}
.wx-toolbar {
	flex: 0 0 48px;
	display: flex;
	justify-content: space-between;
	align-items: center;
	padding: 0 12px;
	width: 100%;
	background-color: var(--wx-background);
	border-radius: 6px 6px 0 0;
	height: 48px;
}
.wx-toolbar .wx-name {
	white-space: nowrap;
	overflow: hidden;
	text-overflow: ellipsis;
	font-weight: var(--wx-font-weight-md);
	font-size: 16px;
}
.wx-toolbar .wx-icons {
	display: flex;
}
.wx-preview {
	display: flex;
	flex-direction: column;
	box-shadow: var(--wx-fm-box-shadow);
	height: 60%;
	margin-bottom: 10px;
	border-radius: 6px;
}
.wx-preview .wx-img-wrapper,
.wx-preview .wx-icon-wrapper {
	border-top: none;
	flex-grow: 1;
	border-radius: 0 0 6px 6px;
}
.wx-preview .wx-icon-wrapper {
	padding: 20px;
}
.wx-preview .wx-img-wrapper {
	height: calc(100% - 48px);
}

.wx-preview .wx-img-wrapper img {
	width: 100%;
	height: 100%;
	object-fit: cover;
}
.wx-img-wrapper,
.wx-icon-wrapper {
	background-color: var(--wx-background);
	display: flex;
	justify-content: center;
	align-items: center;
}
.wx-preview .wx-major-icon {
	font-size: 105px;
	color: var(--wx-color-primary);
}
.wx-img-wrapper img {
	max-width: 100%;
}
.wx-info-panel {
	flex-grow: 1;
	height: calc(40% - 10px);
	border-radius: 6px;
	background-color: var(--wx-background);
	box-shadow: var(--wx-fm-box-shadow);
}
.wx-title {
	display: flex;
	border-bottom: var(--wx-fm-grid-border);
	font-weight: var(--wx-font-weight-md);
	align-items: center;
	justify-content: flex-start;
	padding: 15px;
	font-size: 16px;
}
.wx-list {
	padding: 14px;
	max-height: calc(100% - 51px);
	display: grid;
	grid-template-columns: minmax(40px, max-content) 1fr;
	grid-auto-rows: auto;
	column-gap: 25px;
	overflow-y: auto;
}
.wx-list span {
	padding: 6px;
}
.wx-list .wx-name {
	font-size: 14px;
	font-weight: var(--wx-font-weight-md);
	grid-column: 1 / 2;
	min-width: 60px;
	white-space: nowrap;
	overflow: hidden;
	text-overflow: ellipsis;
}
.wx-list .wx-value {
	grid-column: 2 / 3;
	padding: 6px;
}
.wx-no-info-panel {
	height: 100%;
	width: 100%;
	background-color: var(--wx-background);
	border-radius: 6px;
	box-shadow: var(--wx-fm-box-shadow);
}
.wx-no-info-wrapper {
	height: 100%;
	display: flex;
	flex-direction: column;
	justify-content: center;
	align-items: center;
}
.wx-no-info {
	padding: 5px;
	text-align: center;
}
.wx-no-info .wx-icon-wrapper {
	min-height: 120px;
}
.wx-no-info .wx-major-icon {
	font-size: 120px;
}
.wx-no-info .wx-text {
	font-size: var(--wx-font-size);
	line-height: var(--wx-line-height);
	font-weight: var(--wx-font-weight-md);
	text-align: center;
}
</style>
