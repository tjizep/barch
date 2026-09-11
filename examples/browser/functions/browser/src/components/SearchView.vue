<script setup>
import { computed, inject } from "vue";
import { subscribe } from "@svar-ui/lib-vue";
import Panel from "./Cards/Panel.vue";
import Icon from "./ui/Icon.vue";

const _ = inject("wx-i18n").getGroup("filemanager");
const api = inject("filemanager-store");

const { panels, activePanel } = api.getReactiveState();
const sPanels = subscribe(panels, true);
const sActivePanel = subscribe(activePanel);
const crumbs = computed(() => sPanels.value[sActivePanel.value]._crumbs);

function clearSearch() {
	api.exec("filter-files", {
		text: "",
	});
}
</script>

<template>
	<div class="wx-search">
		<div class="wx-toolbar">
			<div class="wx-back-icon">
				<Icon name="angle-left" :onclick="clearSearch" />
			</div>
			<div class="wx-text">
				{{ _("Search results in") }}
				<template v-for="(crumb, i) in crumbs" :key="crumb.id">
					<template v-if="i">/</template>
					{{ crumb.id == "/" ? _(crumb.name) : crumb.name }}
				</template>
			</div>
		</div>
		<Panel />
	</div>
</template>

<style scoped>
.wx-search {
	display: flex;
	flex-direction: column;
	height: 100%;
	max-height: 100%;
	max-width: 100%;
	flex-shrink: 1;
	padding: 10px;
	padding-top: 0;
}
.wx-toolbar {
	flex: 0 0 48px;
	display: flex;
	justify-content: flex-start;
	align-items: center;
	padding: 0 12px;
	max-width: 100%;
	background-color: var(--wx-background);
	border: 1px solid var(--wx-border);
}

.wx-text {
	font-size: 16px;
}

.wx-back-icon {
	margin-right: 4px;
}
</style>
