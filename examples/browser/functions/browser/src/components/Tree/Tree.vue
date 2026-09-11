<script setup>
defineOptions({ name: "FilemanagerTreeTree" });

import { inject, ref, computed, watchEffect } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import { asDirective } from "@svar-ui/lib-vue";

import { delegateClick } from "@svar-ui/lib-dom";
import Folder from "./Folder.vue";
import { getSelectionOnNavigation } from "@svar-ui/filemanager-store";

const vDelegateClick = asDirective(delegateClick);

const api = inject("filemanager-store");
const { data, panels, activePanel } = api.getReactiveState();

const $data = subscribe(data, true);
const $panels = subscribe(panels, true);
const $activePanel = subscribe(activePanel);

const crumbs = computed(() => $panels.value[$activePanel.value]._crumbs);

function toggle(id) {
	api.exec("open-tree-folder", {
		id,
		mode: !$data.value.byId(id).open,
	});
}

function click(id) {
	const selectedId = getSelectionOnNavigation(id, crumbs.value);
	api.exec("set-path", {
		id,
		panel: $activePanel.value,
		...(selectedId && { selected: [selectedId] }),
	});
}

</script>

<template>
	<ul v-delegate-click="{ click, toggle }">
		<template v-for="folder in $data.byId(0).data" :key="folder.id">
			<Folder v-if="folder.type === 'folder'" :folder="{ ...folder }" />
		</template>
	</ul>
</template>

<style scoped>
ul {
	padding: 0;
	margin: 0;
	height: 100%;
	min-width: 100%;
	width: fit-content;
}
</style>
