<script setup>
import { WillowDark } from "@svar-ui/vue-core";

const props = defineProps({
	fonts: { type: Boolean, default: true },
});
</script>

<template>
    <WillowDark v-if="$slots.default" :fonts="props.fonts">
        <slot />
    </WillowDark>
    <WillowDark v-else :fonts="props.fonts" />
</template>

<style scoped>
:global(.wx-willow-dark-theme) {
	--wx-theme-name: willow-dark;
	color-scheme: dark;

	--wx-fm-background: #2b343b;
	--wx-fm-box-shadow: none;
	--wx-fm-select-background: #384047;
	--wx-fm-grid-border: 1px solid #384047;
	--wx-fm-grid-header-color: var(--wx-background);
	--wx-fm-button-font-color: #9fa1ae;
	--wx-fm-toolbar-height: 56px;
}
</style>
