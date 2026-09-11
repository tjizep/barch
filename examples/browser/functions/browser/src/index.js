import Filemanager from "./components/Filemanager.vue";
import Tooltip from "./components/Tooltip.vue";

import Willow from "./themes/Willow.vue";
import WillowDark from "./themes/WillowDark.vue";

export { Filemanager, Tooltip, Willow, WillowDark };

export { getMenuOptions } from "@svar-ui/filemanager-store";

import { setEnv, env } from "@svar-ui/lib-dom";
setEnv(env);
