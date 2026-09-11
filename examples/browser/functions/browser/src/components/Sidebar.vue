<script setup>
defineOptions({ name: "FilemanagerSidebar" });

import { computed, inject } from "vue";

import { Button } from "@svar-ui/vue-core";
import { DropDownMenu, registerMenuItem } from "@svar-ui/vue-menu";

import Tree from "./Tree/Tree.vue";
import Drive from "./Drive.vue";
import UploadButton from "./UploadButton.vue";

const props = defineProps({
	readonly: { type: Boolean },
	menuOptions: { type: Function },
});

const _ = inject("wx-i18n").getGroup("filemanager");
const { showPrompt } = inject("filemanager-modals");

registerMenuItem("upload", UploadButton);

function handleClick({ action }) {
	if (action) {
		if (action.id === "add-file")
			showPrompt({
				item: {
					type: "file",
					size: 0,
					date: new Date(),
				},
				add: true,
			});
		else if (action.id === "add-folder")
			showPrompt({
				item: {
					type: "folder",
					date: new Date(),
				},
				add: true,
			});
	}
}

const options = computed(() =>
	props.menuOptions("add").map(option => {
		option.text = _(option.text);
		return option;
	})
);
</script>

<template>
	<div class="wx-wrapper">
		<div v-if="!readonly" class="wx-button-box">
			<DropDownMenu :options="options" at="bottom-fit" :onclick="handleClick">
				<Button type="primary block">{{ _("Add New") }}</Button>
			</DropDownMenu>
		</div>
		<div class="wx-tree">
			<Tree />
		</div>
		<Drive />
	</div>
</template>

<style scoped>
.wx-wrapper {
	display: flex;
	flex-direction: column;
	height: 100%;
	background-color: var(--wx-background);
	border-radius: 6px;
	box-shadow: var(--wx-fm-box-shadow);
}

.wx-button-box {
	padding: 8px 8px 0;
}

.wx-tree {
	padding-top: 8px;
	flex-grow: 1;
	overflow-x: auto;
}
</style>
