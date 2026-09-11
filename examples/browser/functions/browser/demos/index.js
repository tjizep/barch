import { createApp } from "vue";
import { router } from "./common/helpers";

import Index from "./common/Index.vue";

import Willow from "../src/themes/Willow.vue";
import WillowDark from "../src/themes/WillowDark.vue";

import { Button, Segmented, Globals, Locale } from "@svar-ui/vue-core";
import "@wx/vue-core/style.css";
import "@wx/vue-uploader/style.css";
import "@wx/vue-menu/style.css";
import "@wx/vue-grid/style.css";

import { setEnv, env } from "@svar-ui/lib-dom";
setEnv(env);

const app = createApp(Index, {
	publicName: "FileManager",
	productTag: "filemanager",
	productLink: "filemanager",
	skins: [
		{
			id: "willow",
			label: "Willow",
			component: Willow,
		},
		{
			id: "willow-dark",
			label: "Dark",
			component: WillowDark,
		},
	],
	Button,
	Segmented,
	Globals,
	Locale,
});

app.use(router);
app.mount("#app");
