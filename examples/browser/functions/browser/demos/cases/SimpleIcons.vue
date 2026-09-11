<script setup>
import { getData, getDrive } from "../data";
import { Filemanager } from "../../src/";
</script>

<template>
	<Filemanager :data="getData()" :drive="getDrive()" icons="simple" />
</template>
