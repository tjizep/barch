<script setup>
import { getData, getDrive } from "../data";
import { Filemanager } from "../../src/";
</script>

<template>
	<div class="filemanager">
		<Filemanager :data="getData()" :drive="getDrive()" />
	</div>
</template>

<style scoped>
.filemanager {
	height: 100%;
	width: 100%;
	--wx-color-primary: rgb(11, 162, 208);
	--wx-fm-background: rgb(207, 209, 221);
	--wx-fm-select-color: rgb(235, 235, 255);
	--wx-table-select-background: rgba(33, 195, 255, 0.1);
	--wx-table-select-focus-background: rgba(33, 195, 255, 0.1);
}

.filemanager > :deep(.wx-filemanager .wx-cards .wx-back) {
	color: rgba(74, 93, 237);
}
</style>
