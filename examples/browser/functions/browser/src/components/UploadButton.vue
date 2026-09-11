<script setup>
import { inject } from "vue";
import { apiKey } from "@svar-ui/vue-uploader";

const props = defineProps({
	option: {},
});

const _ = inject("wx-i18n").getGroup("filemanager");
const uploaderApi = inject(apiKey);

function openFileChooserDialog() {
	uploaderApi.open({});
}
</script>

<template>
	<div class="wx-upload-button" @click="openFileChooserDialog">
		<span>{{ _(option.text) }}</span>
	</div>
</template>

<style scoped>
.wx-upload-button {
	width: 100%;
}
</style>
