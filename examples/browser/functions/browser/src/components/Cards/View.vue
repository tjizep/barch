<script setup>
defineOptions({ name: "FilemanagerCardsView" });

import { inject } from 'vue';
import { subscribe } from '@svar-ui/lib-vue';

import Breadcrumbs from '../Breadcrumbs.vue';
import Panel from './Panel.vue';
import UploadDropArea from '../UploadDropArea.vue';

const api = inject('filemanager-store');
const { activePanel } = api.getReactiveState();
const activePanelVal = subscribe(activePanel);
</script>

<template>
	<div class="wx-wrapper">
		<Breadcrumbs :panel="activePanelVal" />
		<UploadDropArea>
			<Panel />
		</UploadDropArea>
	</div>
</template>

<style scoped>
.wx-wrapper {
	display: flex;
	flex-direction: column;
	height: 100%;
	max-height: 100%;
	max-width: 100%;
	flex-shrink: 1;
}

.wx-wrapper > :deep(.wx-upload-area) {
	height: calc(100% - 48px);
}

.wx-wrapper > :deep(.wx-upload-area) {
	overflow-y: auto;
	border: 1px solid transparent;
	border-radius: 0 0 6px 6px;
}
.wx-wrapper > :deep(.wx-upload-area.wx-active) {
	border: 1px solid var(--wx-color-primary);
}
</style>
