<script setup>
import { inject, computed } from "vue";

const props = defineProps({
	row: {},
});

const _ = inject("wx-i18n").getGroup("filemanager");
const templates = inject("filemanager-store").templates;

const icon = computed(() => templates.icon(props.row, "small"));
</script>

<template>
	<div class="wx-name-cell">
		<template v-if="props.row.id === '/wx-filemanager-parent-link'">
			<i class="wxi-arrow-left"></i>
			<span class="wx-name"> {{ _("Back to parent folder") }}</span>
		</template>
		<template v-else>
			<img
				v-if="icon"
				class="wx-icon"
				alt=""
				:src="icon"
				height="24px"
				width="24px"
			/>
			<i v-else :class="`wxi-${props.row.type}`"></i>
			<span class="wx-name"> {{ props.row.name }} </span>
		</template>
	</div>
</template>

<style scoped>
.wx-name-cell {
	padding: 0 4px;
	display: flex;
	align-items: center;
	height: 100%;
	overflow: hidden;
	flex-shrink: 0;
	text-overflow: clip;
}
i,
.wx-icon {
	margin-right: 10px;
	display: flex;
	align-items: center;
}

i {
	font-size: 24px;
	color: var(--wx-color-primary);
}
.wx-name {
	white-space: nowrap;
	overflow: hidden;
	text-overflow: ellipsis;
}
</style>
