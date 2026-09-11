<script setup>
defineOptions({ name: "FilemanagerToolbar" });

import { inject } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import { Segmented, TwoState } from "@svar-ui/vue-core";
import { setID } from "@svar-ui/lib-dom";
import Search from "./ui/Search.vue";
import Icon from "./ui/Icon.vue";

const props = defineProps({
	narrowMode: { type: Boolean, default: false },
	onshowtree: { type: Function },
});

const api = inject("filemanager-store");

const { mode, preview: info, search: searchValue } = api.getReactiveState();

const modeVal = subscribe(mode);
const infoVal = subscribe(info);
const searchVal = subscribe(searchValue);

const _ = inject("wx-i18n").getGroup("filemanager");

const options = [
	{ icon: "wxi-view-sequential", id: "table" },
	{ icon: "wxi-view-grid", id: "cards" },
	{ icon: "wxi-view-column", id: "panels" },
];

function changeMode({ value }) {
	api.exec("set-mode", { mode: value });
}
function toggleInfo(e) {
	e.preventDefault();
	api.exec("show-preview", { mode: !infoVal.value });
}
function search(e) {
	api.exec("filter-files", { text: e.value });
}
</script>

<template>
	<div class="wx-toolbar">
		<div
			:class="[
				'wx-left',
				{ 'wx-left-narrow': props.narrowMode },
			]"
		>
			<template v-if="!props.narrowMode">
				<div class="wx-name">{{ _("Files") }}</div>
			</template>
			<template
				v-else-if="
					!(modeVal === 'panels' || modeVal === 'search')
				"
			>
				<div
					class="wx-sidebar-icon"
					:data-id="setID('toggle-tree')"
				>
					<Icon
						:onclick="
							() => props.onshowtree && props.onshowtree()
						"
						name="subtask"
					/>
				</div>
			</template>
			<Search :onsearch="search" :value="searchVal" />
		</div>

		<div class="wx-right">
			<div v-if="!props.narrowMode" class="wx-preview-icon">
				<TwoState :onclick="toggleInfo" :value="infoVal">
					<i class="wxi-eye"></i>
				</TwoState>
			</div>
			<div class="wx-modes">
				<Segmented
					:value="modeVal"
					:options="options"
					:onchange="changeMode"
				/>
			</div>
		</div>
	</div>
</template>

<style scoped>
.wx-toolbar {
	height: var(--wx-fm-toolbar-height);
	display: flex;
	justify-content: space-between;
	align-items: center;
	padding: 0 12px;
	max-width: 100%;
	background-color: var(--wx-background);
	box-shadow: var(--wx-fm-box-shadow);
	gap: 8px;
}

.wx-left,
.wx-right {
	display: flex;
}

.wx-left {
	align-items: center;
	width: 35%;
	justify-content: space-between;
}
.wx-left-narrow {
	width: 70%;
}
.wx-name {
	margin-right: 20px;
	font-size: 16px;
	font-weight: var(--wx-font-weight-md);
}

.wx-sidebar-icon {
	margin-right: 20px;
}

.wx-preview-icon {
	display: flex;
	justify-content: center;
	align-items: center;
	margin-right: 20px;

	--wx-button-icon-size: 22px;
	--wx-button-line-height: 25px;
	--wx-button-padding: 4px 10px;
	--wx-button-border-radius: 6px;
}

.wx-preview-icon i {
	position: relative;
	display: inline-block;
	vertical-align: top;
	font-size: var(--wx-button-icon-size);
	line-height: 1;
	height: var(--wx-button-line-height);
}

.wx-preview-icon i:before {
	display: block;
	position: relative;
	top: 50%;
	transform: translateY(-50%);
	color: var(--wx-fm-button-font-color);
}

.wx-modes {
	--wx-button-font-color: var(--wx-fm-button-font-color);
	--wx-segmented-padding: 1.5px;
	--wx-segmented-background-hover: linear-gradient(
		rgba(0, 0, 0, 0.1) 0%,
		rgba(0, 0, 0, 0.1) 100%
	);
	--wx-button-icon-size: 22px;
	--wx-segmented-border-radius: 6px;
	--wx-segmented-border: none;
}
</style>
