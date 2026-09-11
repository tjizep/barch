<script setup>
defineOptions({ name: "FilemanagerCardsPanel" });

import { inject, computed } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import { delegateClick, setID } from "@svar-ui/lib-dom";
import { asDirective } from "@svar-ui/lib-vue";
import Item from "./Item.vue";

const vDelegateClick = asDirective(delegateClick);

const api = inject("filemanager-store");
const _ = inject("wx-i18n").getGroup("filemanager");

const { panels, activePanel, mode } = api.getReactiveState();

const panelsVal = subscribe(panels, true);
const activePanelVal = subscribe(activePanel);
const modeVal = subscribe(mode);

const files = computed(() => panelsVal.value[activePanelVal.value]._files);
const selected = computed(
	() => panelsVal.value[activePanelVal.value].selected
);
const path = computed(() => panelsVal.value[activePanelVal.value].path);
const crumbs = computed(() => panelsVal.value[activePanelVal.value]._crumbs);
const selectNavigation = computed(
	() => panelsVal.value[activePanelVal.value]._selectNavigation
);

function click(id, e) {
	const ctrl = e && (e.ctrlKey || e.metaKey);
	const shift = e && e.shiftKey;

	if (id === "/wx-filemanager-parent-link") {
		if (selected.value.length && (ctrl || shift)) return;
		api.exec("select-file", {
			type: "navigation",
		});
		return;
	}

	const isFile = id !== "body";
	let newSelection = !isFile && e ? null : id;

	const actionClick =
		e.target.className.indexOf("wx-more") !== -1 ||
		e.target.className.indexOf("wxi-dots-v") !== -1;

	api.exec("select-file", {
		id: newSelection,
		toggle: ctrl && !actionClick,
		range: shift && !actionClick,
		panel: activePanelVal.value,
	});
}

function backToParent() {
	if (crumbs.value.length > 1) {
		api.exec("set-path", {
			id: crumbs.value[crumbs.value.length - 2].id,
			panel: activePanelVal.value,
			selected: [crumbs.value[crumbs.value.length - 1].id],
		});
	}
}

function dblclick(id) {
	if (id === "/wx-filemanager-parent-link") {
		return backToParent();
	}

	if (modeVal.value === "search") {
		api.exec("filter-files", {
			text: "",
		});
	}

	const item = files.value.find(a => a.id === id);

	if (item) {
		if (item.type == "folder") {
			api.exec("set-path", {
				id: item.id,
				panel: activePanelVal.value,
			});
		} else {
			api.exec("open-file", {
				id: item.id,
			});
		}
	}
}

function applySelection(id, ev) {
	if (
		!selected.value?.length ||
		!selected.value.filter(i => i?.id === id).length > 0
	) {
		click(id, ev);
	}
}

const renderedFiles = computed(() =>
	path.value !== "/"
		? [
				{
					id: "/wx-filemanager-parent-link",
					name: _("Back to parent folder"),
					navigation: selectNavigation.value,
				},
				...files.value,
			]
		: files.value
);
</script>

<template>
	<div v-if="modeVal === 'search' && !renderedFiles.length" class="wx-not-found">
		<div class="wx-not-found-text">{{ _("Looks like nothing is here") }}</div>
	</div>
	<div
		v-else
		tabindex="0"
		:class="['wx-cards', { 'wx-has-back-link': path !== '/' && modeVal !== 'search' }]"
		:data-id="setID('body')"
		v-delegate-click="{ click, dblclick, context: applySelection }"
	>
		<Item v-for="child in renderedFiles" :key="child.id" :item="child" />
	</div>
</template>

<style scoped>
.wx-cards {
	flex-grow: 1;
	flex-wrap: wrap;
	height: 100%;
	border-top: none;
	padding: 30px 20px 10px;
	display: flex;
	align-items: flex-start;
	overflow-y: auto;
	align-content: flex-start;
	outline: none;
}
.wx-cards.wx-has-back-link {
	padding: 0 20px 10px;
}
.wx-not-found-text {
	text-align: center;
	color: var(--wx-color-font-alt);
}
.wx-not-found {
	display: flex;
	flex-direction: column;
	justify-content: center;
	align-items: center;
	gap: 5px;
	height: 100%;
}
</style>
