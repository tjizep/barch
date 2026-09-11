<script setup>
defineOptions({ name: "FilemanagerTreeFolder" });

import { computed, inject } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import { setID } from "@svar-ui/lib-dom";

const api = inject("filemanager-store");
const { panels, activePanel } = api.getReactiveState();

const sPanels = subscribe(panels, true);
const sActivePanel = subscribe(activePanel);

const path = computed(() => sPanels.value[sActivePanel.value].path);

const props = defineProps({
	folderIcon: { type: [Boolean, String], default: false },
	folder: { type: Object, default: () => ({}) },
});

const locale = inject("wx-i18n");
const _ = locale.getGroup("filemanager");

const hasFolders = computed(
	() => !!props.folder.data?.find(i => i.type === "folder")
);
const name = computed(() =>
	props.folder.id == "/" ? _(props.folder.name) : props.folder.name
);
const padding = computed(() =>
	props.folder.$level > 1 ? (props.folder.$level - 1) * 20 : 0
);
const open = computed(() => props.folder.open);
</script>

<template>
	<li
		:data-id="setID(props.folder.id)"
		class="wx-folder"
		:class="{ 'wx-selected': path === props.folder.id }"
		:style="{ 'padding-left': padding + 'px' }"
	>
		<i
			v-if="hasFolders"
			:class="[
				'wx-toggle',
				open ? 'wxi-angle-down' : 'wxi-angle-right',
			]"
			data-action="toggle"
		></i>
		<span v-else class="wx-toggle-placeholder"></span>
		<i :class="folderIcon || 'wxi-folder'"></i>
		<span class="wx-name"> {{ name }} </span>
	</li>

	<template
		v-if="open && props.folder.data && props.folder.data.length && hasFolders"
	>
	{{  console.log(JSON.stringify(props.folder.data.map(a => a.name))) }}
		<template v-for="child in props.folder.data" :key="child.id">
			<FilemanagerTreeFolder
				v-if="child.type === 'folder'"
				:folder="{ ...child }"
			/>
		</template>
	</template>
</template>

<style scoped>
.wx-folder {
	display: flex;
	align-items: center;
	cursor: default;
	letter-spacing: 0.2px;
	width: 100%;
	height: 32px;
	vertical-align: top;
	white-space: nowrap;
	position: relative;
}

.wx-selected {
	background-color: var(--wx-fm-select-background);
}
i {
	font-size: 22px;
	margin-right: 8px;
	max-height: 100%;
	color: var(--wx-color-primary);
}
.wx-toggle {
	cursor: pointer;
	color: var(--wx-icon-color);
	font-size: 24px;
	margin-right: -2px;
}
.wx-toggle-placeholder {
	width: 23px;
	flex-shrink: 0;
}
.wx-name {
	padding-right: 8px;
}
</style>
