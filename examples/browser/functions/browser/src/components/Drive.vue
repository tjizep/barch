<script setup>
defineOptions({ name: "FilemanagerDrive" });

import { computed, inject } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import { formatSize } from "@svar-ui/filemanager-store";

const api = inject("filemanager-store");
const drive = subscribe(api.getReactiveState().drive, true);
const _ = inject("wx-i18n").getGroup("filemanager");

const used = computed(() => (drive.value ? drive.value.used : ""));
const total = computed(() => (drive.value ? drive.value.total : ""));
</script>

<template>
	<div v-if="used && total" class="wx-drive">
		<progress :value="used" :max="total" class="wx-progress"></progress>
		<p>{{ formatSize(used) }} {{ _("of") }} {{ formatSize(total) }} {{ _("used") }}</p>
	</div>
</template>

<style scoped>
.wx-drive {
	display: flex;
	flex-direction: column;
	justify-content: center;
	padding: 8px;
}
.wx-progress {
	width: 100%;
	height: 8px;
	border-radius: 20px;
	background-color: var(--wx-button-background);
	border: none;
}
.wx-progress[value]::-webkit-progress-bar {
	border-radius: 20px;
	background-color: var(--wx-fm-progress-bar-color);
}
.wx-progress[value]::-moz-progress-bar {
	background-color: var(--wx-color-primary);
	border-radius: 10px;
}

.wx-progress[value]::-webkit-progress-value {
	background-color: var(--wx-color-primary);
	border-radius: 10px;
}
.wx-drive p {
	margin: 20px 0;
}
</style>
