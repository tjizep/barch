<script setup>
defineOptions({ name: "DemoTooltips" });
import { ref } from "vue";
import { getData, getDrive } from "../data";
import { Filemanager, Tooltip } from "../../src/";

const api = ref(null);
const data = getData();
data.push({
	id: "/Hover me for tooltip. Visible for entries with overflow.txt",
	size: 1025,
	date: new Date(2023, 11, 1, 14, 45),
	type: "file",
});
</script>

<template>
	<Tooltip overflow :api="api">
		<Filemanager ref="api" :data="data" :drive="getDrive()" />
	</Tooltip>
</template>
