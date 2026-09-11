<script setup>
defineOptions({ name: "FilemanagerComponentsBreadcrumbs" });

import { ref, computed, watchEffect, inject } from "vue";
import { delegateClick, setID } from "@svar-ui/lib-dom";
import { asDirective } from "@svar-ui/lib-vue";
import { getSelectionOnNavigation } from "@svar-ui/filemanager-store";
import { subscribe } from "@svar-ui/lib-vue";
import Icon from "./ui/Icon.vue";

const vDelegateClick = asDirective(delegateClick);

const props = defineProps({
	panel: {},
});

const api = inject("filemanager-store");
const _ = inject("wx-i18n").getGroup("filemanager");

const { panels } = api.getReactiveState();
const panelsRef = subscribe(panels, true);
const files = computed(() => panelsRef.value[props.panel]._files);
const crumbs = computed(() => panelsRef.value[props.panel]._crumbs);

function click(id) {
	const selectedId = getSelectionOnNavigation(id, crumbs.value);
	api.exec("set-path", {
		id,
		panel: props.panel,
		...(selectedId && { selected: [selectedId] }),
	});
}

const loading = ref(null);

watchEffect(() => {
	if (files.value) loading.value = null;
});

function refresh() {
	loading.value = true;
	api.exec("request-data", {
		id: crumbs.value[crumbs.value.length - 1].id,
	});
	//if data was not loaded - stop spinner anyway
	setTimeout(() => {
		loading.value = null;
	}, 5000);
}
</script>

<template>
	<div
		class="wx-breadcrumbs"
		v-delegate-click="{ click }"
		data-menu-ignore="true"
	>
		<div class="wx-refresh-icon">
			<Icon name="refresh" :spin="!!loading" :onclick="refresh" />
		</div>
		<template v-for="(crumb, i) in crumbs" :key="crumb.id">
			<Icon v-if="i" name="angle-right" />
			<div
				class="wx-item"
				:data-id="setID(crumb.id)"
				data-menu-ignore="true"
			>
				{{ crumb.id == "/" ? _(crumb.name) : crumb.name }}
			</div>
		</template>
	</div>
</template>

<style scoped>
.wx-breadcrumbs {
	flex: 0 0 48px;
	display: flex;
	justify-content: flex-start;
	align-items: center;
	padding: 0 4px;
	max-width: 100%;
	border-radius: 6px 6px 0 0;
	background-color: var(--wx-background);
	font-size: 16px;
	overflow: hidden;
}

.wx-refresh-icon {
	margin-right: 4px;
}

.wx-item {
	cursor: pointer;
	font-size: 16px;
	font-weight: 300;
	white-space: nowrap;
	overflow: hidden;
	text-overflow: ellipsis;
}
.wx-item:hover {
	color: var(--wx-color-primary);
}
</style>
