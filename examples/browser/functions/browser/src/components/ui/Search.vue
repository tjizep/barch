<script setup>
defineOptions({ name: "FilemanagerUiSearch" });

import { ref, computed, inject } from 'vue';

const props = defineProps({
	value: { default: '' },
	onsearch: { type: Function },
});

const _ = inject('wx-i18n').getGroup('filemanager');

const node = ref(null);
const icon = computed(() => (props.value !== '' ? 'wxi-close' : 'wxi-search'));

function oninput(e) {
	const val = e.target.value;
	props.onsearch?.({ value: val });
}

function clear() {
	props.onsearch?.({ value: '' });
	node.value.focus();
}
</script>

<template>
	<div class="wx-search-input">
		<i :class="['wx-icon', icon]" @click="clear"></i>
		<input
			type="text"
			class="wx-text"
			ref="node"
			:value="props.value"
			@input="oninput"
			:placeholder="_('Search')"
		/>
	</div>
</template>

<style scoped>
.wx-search-input {
	position: relative;
	width: 100%;
	height: 30px;
}

.wx-icon {
	display: flex;
	justify-content: center;
	align-items: center;
	position: absolute;
	top: 4px;
	bottom: 4px;
	right: 1px;
	width: 25px;
	color: #94a1b3;
	font-size: 20px;
	cursor: pointer;
}

.wx-text {
	display: block;
	width: 100%;
	height: 30px;
	padding-left: 12px;
	border: var(--wx-border);
	outline: none;
	background-color: var(--wx-background);
	border-radius: 2px;
}

.wx-text::placeholder {
	color: #94a1b3;
}

.wx-text:focus {
	border: 1px solid var(--wx-color-primary);
}
</style>
