<script setup>
defineOptions({ name: "FilemanagerTableView" });

import { computed, inject } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import { asDirective } from "@svar-ui/lib-vue";
import { formatSize } from "@svar-ui/filemanager-store";
import { delegateClick, dateToString, setID } from "@svar-ui/lib-dom";
import { Grid } from "@svar-ui/vue-grid";

import Breadcrumbs from "../Breadcrumbs.vue";
import NameCell from "./NameCell.vue";
import UploadDropArea from "../UploadDropArea.vue";

const vDelegateClick = asDirective(delegateClick);

const props = defineProps({
	panel: {},
	active: { type: Boolean, default: false },
	onclick: { type: Function },
	oncontextmenu: { type: Function },
});

const api = inject("filemanager-store");

const locale = inject("wx-i18n");
const _ = locale.getGroup("filemanager");
const format = dateToString("%d %M %Y", locale.getRaw().calendar);

const { panels } = api.getReactiveState();
const panelsV = subscribe(panels, true);

const selection = computed(() => panelsV.value[props.panel].selected);
const path = computed(() => panelsV.value[props.panel].path);
const crumbs = computed(() => panelsV.value[props.panel]._crumbs);
const sorts = computed(() => panelsV.value[props.panel]._sorts);
const selectNavigation = computed(
	() => panelsV.value[props.panel]._selectNavigation
);
const files = computed(() => panelsV.value[props.panel]._files);

const columns = [
	{
		id: "name",
		header: _("Name"),
		flexgrow: 3,
		sort: true,
		resize: true,
		cell: NameCell,
	},
	{
		id: "size",
		header: _("Size"),
		width: 100,
		sort: true,
		resize: true,
		template: v => (typeof v === "number" ? formatSize(v) : ""),
	},
	{
		id: "date",
		header: _("Date"),
		width: 120,
		sort: true,
		resize: true,
		template: v => (v ? format(v) : ""),
	},
];

let sortClick = null;
let resizeClick = null;

const tableSelection = computed(() =>
	selectNavigation.value
		? ["/wx-filemanager-parent-link"]
		: [...selection.value]
);

function click(id, e) {
	//[FIXME] grip is wx-table classname which may change
	if (e && e.target.className.indexOf("wx-grip") !== -1) return;

	const ctrl = e && (e.ctrlKey || e.metaKey);
	const shift = e && e.shiftKey;

	if (id === "/wx-filemanager-parent-link") {
		if (selection.value.length && (ctrl || shift)) return;
		api.exec("select-file", {
			type: "navigation",
		});
		return;
	}

	const isFile = id !== "body";
	let newSelection = !isFile && e ? null : id;

	if (!sortClick && !resizeClick) {
		api.exec("select-file", {
			id: newSelection,
			toggle: ctrl,
			range: shift,
			panel: props.panel,
		});
	} else sortClick = resizeClick = null;
}

function backToParent() {
	if (crumbs.value.length > 1) {
		api.exec("set-path", {
			id: crumbs.value[crumbs.value.length - 2].id,
			panel: props.panel,
			selected: [crumbs.value[crumbs.value.length - 1].id],
		});
	}
}

function dblclick(id) {
	if (id === "/wx-filemanager-parent-link") {
		return backToParent();
	}
	const item = files.value.find(a => a.id === id);

	if (item) {
		if (item.type == "folder") {
			api.exec("set-path", {
				id: item.id,
				panel: props.panel,
			});
		} else {
			api.exec("open-file", {
				id: item.id,
			});
		}
	}
}

function handleSort(e) {
	const col = e.key;
	const prevSort = sorts.value[path.value];
	let order = !prevSort ? "desc" : "asc";

	if (prevSort && prevSort.key === col) {
		order = prevSort.order === "asc" ? "desc" : "asc";
	}

	api.exec("sort-files", {
		key: col,
		order,
		panel: props.panel,
		path: path.value,
	});
}

function initTable(api) {
	api.intercept("sort-rows", e => {
		sortClick = true;
		handleSort(e);
		return false;
	});

	api.intercept("select-row", () => false);

	api.on("resize-column", () => (resizeClick = true));

	api.intercept("hotkey", () => false);
}

const renderedFiles = computed(() =>
	path.value !== "/"
		? [
				{
					id: "/wx-filemanager-parent-link",
					name: _("Back to parent folder"),
				},
				...files.value,
			]
		: files.value
);

const sortMarks = computed(() => {
	if (renderedFiles.value && sorts.value[path.value]) {
		const { key, order } = sorts.value[path.value];
		return { [key]: { order } };
	}
	return undefined;
});
</script>

<template>
	<div :onclick="props.onclick" :oncontextmenu="props.oncontextmenu" class="wx-wrapper">
		<Breadcrumbs :panel="props.panel" />
		<div
			:data-id="setID('body')"
			:class="['wx-list', { 'wx-active': props.active }]"
			v-delegate-click="{ click, dblclick }"
		>
			<UploadDropArea>
				<Grid
					:init="initTable"
					:data="renderedFiles"
					:columns="columns"
					:selected-rows="tableSelection"
					:column-style="() => 'wx-each-cell'"
					:sizes="{ rowHeight: 42, headerHeight: 42 }"
					:sort-marks="sortMarks"
				/>
			</UploadDropArea>
		</div>
	</div>
</template>

<style scoped>
.wx-wrapper {
	display: flex;
	flex-direction: column;
	height: 100%;
	max-height: 100%;
	max-width: 100%;
	box-shadow: var(--wx-fm-box-shadow);
	border-radius: 6px;
}

.wx-list {
	height: calc(100% - 50px);
}
.wx-list > :deep(.wx-upload-area .wx-grid) {
	--wx-table-cell-border: var(--wx-fm-grid-border);
	--wx-table-header-border: var(--wx-fm-grid-border);
	--wx-table-header-cell-border: var(--wx-fm-grid-border);
}
.wx-list > :deep(.wx-upload-area .wx-body .wx-each-cell) {
	border-right: none;
}
.wx-list
	> :deep(.wx-upload-area .wx-header .wx-cell:first-child .wx-text) {
	padding: 0 6px;
}
.wx-list > :deep(.wx-upload-area .wx-table) {
	border-radius: 0 0 6px 6px;
}

.wx-list.wx-active > :deep(.wx-upload-area .wx-grid),
.wx-list > :deep(.wx-upload-area.wx-active .wx-grid) {
	--wx-table-cell-border: 1px solid var(--wx-color-primary);
}
.wx-list.wx-active > :deep(.wx-upload-area .wx-row) {
	--wx-table-cell-border: var(--wx-fm-grid-border);
}

.wx-list > :deep(.wx-upload-area.wx-active .wx-row) {
	background: var(--wx-color-primary-selected);
	--wx-table-cell-border: 1px solid var(--wx-color-primary-selected);
}
/*switch off focus due to filamanager own navigation system*/
.wx-list > :deep(.wx-upload-area .wx-grid .wx-cell) {
	outline: none;
}

/* temp hack to align toolbar and table body (with 1.75px full match) */
.wx-list > :deep(.wx-upload-area) {
	border-right: 1px solid transparent;
}
</style>
