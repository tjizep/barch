<script setup>
defineOptions({ name: "FilemanagerModals" });

import { ref, computed, inject, provide } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import { Globals, Modal, Text } from "@svar-ui/vue-core";

const _ = inject("wx-i18n").getGroup("filemanager");
const api = inject("filemanager-store");

const { panels: rPanels, activePanel: rActivePanel } =
	api.getReactiveState();

const sPanels = subscribe(rPanels, true);
const sActivePanel = subscribe(rActivePanel);

const path = computed(() => sPanels.value[sActivePanel.value].path);

const prompt = ref(null);
const confirm = ref(null);
const value = ref("");
const initialName = ref("");
const error = ref(false);

function promptOk() {
	const name = value.value.trim();
	if (!name) {
		error.value = true;
		return;
	}

	if (prompt.value.add) {
		api.exec("create-file", {
			file: {
				...prompt.value.item,
				name,
			},
			parent: path.value,
		});
	} else {
		if (initialName.value !== name)
			api.exec("rename-file", { id: prompt.value.item.id, name });
	}

	closePrompt();
}

function closePrompt() {
	prompt.value = null;
	error.value = null;
	initialName.value = "";
	value.value = "";
}

function confirmOk() {
	api.exec("delete-files", { ids: confirm.value.selected });
	confirm.value = null;
}

provide("filemanager-modals", {
	showPrompt(config) {
		initialName.value = value.value =
			config.item.name ||
			(config.item.type === "folder"
				? _("New folder")
				: `${_("New file")}.txt`);
		prompt.value = { ...config };
	},
	showConfirm(config) {
		confirm.value = { ...config };
	},
});
</script>

<template>
	<Globals>
		<slot />
	</Globals>

	<Modal
		v-if="prompt"
		:title="_(`Enter ${prompt.item.type} name`)"
		:onconfirm="promptOk"
		:oncancel="closePrompt"
	>
		<!-- [todo] add selection mask to exclude extensions as a Text feature -->
		<Text :error="error" :select="true" :focus="true" v-model:value="value" />
	</Modal>

	<Modal
		v-if="confirm"
		:title="_('Are you sure you want to delete these items:')"
		:onconfirm="confirmOk"
		:oncancel="() => (confirm = null)"
	>
		<ul v-if="confirm.selected" class="wx-list">
			<li v-for="item in confirm.selected" :key="item">{{ item }}</li>
		</ul>
	</Modal>
</template>

<style scoped>
.wx-list {
	text-align: left;
	padding-left: 20px;
	max-height: 300px;
	overflow: auto;
}

.wx-list li {
	font-weight: var(--wx-font-weight-md);
}
</style>
