<script setup>
defineOptions({ name: "FilemanagerThemesWillow" });

import { Willow } from "@svar-ui/vue-core";

const props = defineProps({
	fonts: { type: Boolean, default: true },
});
</script>

<template>
    <Willow v-if="$slots.default" :fonts="props.fonts">
		<slot />
	</Willow>
	<Willow v-else :fonts="props.fonts" />
</template>

<style scoped>
:global(.wx-willow-theme) {
	--wx-theme-name: willow;

	--wx-fm-background: #f2f3f7;
	--wx-fm-box-shadow: 0px 1px 2px rgba(44, 47, 60, 0.06),
		0px 3px 10px rgba(44, 47, 60, 0.12);
	--wx-fm-select-background: #eaedf5;
	--wx-fm-grid-border: 1px solid #ededf1;
	--wx-fm-grid-header-color: #fafafb;
	--wx-fm-button-font-color: #9fa1ae;
	--wx-fm-toolbar-height: 56px;
}
</style>
