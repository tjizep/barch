<script setup>
import { ref, inject } from "vue";
import { apiKey } from "@svar-ui/vue-uploader";
import { locateAttr } from "@svar-ui/lib-dom";
import { subscribe, asDirective } from "@svar-ui/lib-vue";

const api = inject("filemanager-store");
const activePanel = subscribe(api.getReactiveState().activePanel);

const uploaderApi = inject(apiKey);

const self = ref(null);

const apiSettings = {
	selected: () => {
		const panel = locateAttr(self.value, "data-panel");
		if (panel && panel != activePanel.value) {
			api.exec("set-active-panel", {
				panel: panel * 1,
			});
		}
	},
	dragEnter: () => self.value.classList.toggle("wx-active"),
	dragLeave: () => self.value.classList.toggle("wx-active"),
};

const vDroparea = asDirective(uploaderApi.droparea);
</script>

<template>
	<div
		class="wx-upload-area"
		:class="{ 'wx-active': false }"
		ref="self"
		v-droparea="{ ...apiSettings }"
	>
		<slot />
	</div>
</template>

<style scoped>
.wx-upload-area {
	height: 100%;
}
.wx-upload-area.wx-active {
	background: var(--wx-color-primary-selected);
}
</style>
