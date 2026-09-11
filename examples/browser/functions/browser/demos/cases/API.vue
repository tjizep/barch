<script setup>
import { ref } from "vue";
import { getData, getDrive } from "../data";
import { Filemanager } from "../../src/";
import { Button } from "@svar-ui/vue-core";

const api = ref(null);
let serializedData = [];

function serialize() {
	serializedData = api.value.serialize("/Code");
	api.value.exec("provide-data", {
		id: "/Code",
		data: [],
	});
}

function parse() {
	api.value.exec("provide-data", {
		id: "/Code",
		data: serializedData,
	});
}
</script>

<template>
	<div class="demo">
		<div class="bar">
			<Button :onclick="serialize">
Serialize and clear the "Code" folder
</Button>
			<Button :onclick="parse">Load data back</Button>
		</div>
		<Filemanager
			ref="api"
			:data="getData()"
			:drive="getDrive()"
			:panels="[{ path: '/Code' }]"
		/>
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
	padding: 5px;
}
</style>
