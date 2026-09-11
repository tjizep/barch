<script setup>
defineOptions({ name: "DemoLocales" });
import { ref } from "vue";
import { Locale, Segmented } from "@svar-ui/vue-core";
import { cn } from "@svar-ui/filemanager-locales";
import { cn as cnCore } from "@svar-ui/core-locales";

import { getData, getDrive } from "../data";
import { Filemanager } from "../../src/";

const lang = ref("en");
const options = [
	{ id: "en", label: "EN" },
	{ id: "cn", label: "CN" },
];
</script>

<template>
	<div class="demo">
		<div class="bar">
			<Segmented :options="options" v-model:value="lang" />
		</div>
		<Filemanager
			v-if="lang === 'en'"
			:data="getData()"
			:drive="getDrive()"
/>
		<Locale v-if="lang === 'cn'" :words="{ ...cn, ...cnCore }">
			<Filemanager :data="getData()" :drive="getDrive()" />
		</Locale>
	</div>
</template>

<style scoped>
.demo {
	height: 100%;
	overflow: hidden;
	display: flex;
	flex-direction: column;
}

.bar {
	height: 50px;
	padding: 5px 10px;
}
</style>
