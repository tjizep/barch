<script setup>
import { getData, getDrive } from "../data";
import { Filemanager } from "../../src/";

const panels = [
	{
		path: "/Code",
		selected: ["/Code/Datepicker"],
	},
	{
		path: "/",
		selected: ["/Music"],
	},
];
</script>

<template>
	<Filemanager :data="getData()" :drive="getDrive()" mode="panels" :panels="panels" />
</template>
