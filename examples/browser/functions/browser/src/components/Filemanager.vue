<script setup>
defineOptions({ name: "FilemanagerFilemanager" });

import { provide, watch, useAttrs } from "vue";

import Layout from "./Layout.vue";

import { Locale } from "@svar-ui/vue-core";
import { en } from "@svar-ui/filemanager-locales";

// stores
import { EventBusRouter } from "@svar-ui/lib-state";
import { DataStore, getMenuOptions } from "@svar-ui/filemanager-store";
import Modals from "./Modals.vue";
import { whitelist } from "../icons";

const props = defineProps({
	data: { default: () => [] },
	mode: { default: "cards" },
	drive: { default: null },
	preview: { type: Boolean, default: false },
	panels: { default: () => [] },
	activePanel: { default: 0 },
	readonly: { type: Boolean, default: false },
	menuOptions: { type: Function, default: getMenuOptions },
	extraInfo: { type: Function, default: null },
	init: { type: Function, default: null },
	icons: {
		type: [Function, String],
		default: () => function (file, size) {
			const { type, ext } = file;

			if (type === "folder") return false;

			let icon;

			if (type && type !== "file" && whitelist[type]) {
				icon = type;
			} else if (ext) {
				icon = whitelist[ext] ? ext : "file";
			} else icon = "unknown";

			return `https://cdn.svar.dev/icons/filemanager/vivid/${size}/${icon}.svg`;
		},
	},
	previews: { type: Function, default: null },
});

const attrs = useAttrs();

// init stores
const dataStore = new DataStore();

// define event route
let firstInRoute = dataStore.in;

const dash = /-/g;
let lastInRoute = new EventBusRouter((a, b) => {
	const name = "on" + a.replace(dash, "");
	if (attrs[name]) {
		attrs[name](b);
	}
});
firstInRoute.setNext(lastInRoute);

// public API
const getState = dataStore.getState.bind(dataStore);
const getReactiveState = dataStore.getReactive.bind(dataStore);
const getStores = () => ({ data: dataStore });
const exec = firstInRoute.exec;
const setNext = ev => (lastInRoute = lastInRoute.setNext(ev));
const intercept = firstInRoute.intercept.bind(firstInRoute);
const on = firstInRoute.on.bind(firstInRoute);
const detach = firstInRoute.detach.bind(firstInRoute);
const getFile = id => dataStore.getFile(id);
const serialize = id => dataStore.serialize(id);

defineExpose({
	getState,
	getReactiveState,
	getStores,
	exec,
	setNext,
	intercept,
	on,
	detach,
	getFile,
	serialize,
});

const api = {
	getState,
	getReactiveState,
	getStores,
	exec,
	setNext,
	intercept,
	on,
	detach,
	getFile,
	serialize,
};

const none = () => null;
// common API available in components
provide("filemanager-store", {
	getReactiveState: dataStore.getReactive.bind(dataStore),
	exec: firstInRoute.exec.bind(firstInRoute),
	templates: {
		preview: props.previews || none,
		icon: props.icons == "simple" ? none : props.icons,
	},
	getFile: dataStore.getFile.bind(dataStore),
});

let init_once = true;
const reinitStore = () => {
	dataStore.init({
		data: props.data,
		mode: props.mode,
		drive: props.drive,
		preview: props.preview,
		panels: props.panels,
		activePanel: props.activePanel,
	});

	if (init_once && props.init) {
		props.init(api);
		init_once = false;
	}
};
reinitStore();
watch(
	() => [
		props.data,
		props.mode,
		props.drive,
		props.preview,
		props.panels,
		props.activePanel,
	],
	reinitStore
);
</script>

<template>
	<Locale :words="en" :optional="true">
		<Modals>
			<Layout
				:readonly="props.readonly"
				:menu-options="props.menuOptions"
				:extra-info="props.extraInfo"
			/>
		</Modals>
	</Locale>
</template>
