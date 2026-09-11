import BasicInit from "./cases/BasicInit.vue";
import PathAndSelection from "./cases/PathAndSelection.vue";
import ContextMenu from "./cases/ContextMenu.vue";
import Readonly from "./cases/Readonly.vue";
import CustomStyles from "./cases/CustomStyles.vue";
import SimpleIcons from "./cases/SimpleIcons.vue";
import Locales from "./cases/Locales.vue";
import API from "./cases/API.vue";
import ExtraInfo from "./cases/ExtraInfo.vue";
import Tooltips from "./cases/Tooltips.vue";
import BackendData from "./cases/BackendData.vue";
import DataProvider from "./cases/DataProvider.vue";
import BackendFilter from "./cases/BackendFilter.vue";

export const links = [
	["/base/:skin", "Basic File Manager", BasicInit, "BasicInit"],
	[
		"/selection/:skin",
		"Initial path/selection",
		PathAndSelection,
		"PathAndSelection",
	],
	["/context/:skin", "Custom context menu", ContextMenu, "ContextMenu"],
	["/readonly/:skin", "Readonly mode", Readonly, "Readonly"],
	["/custom-styles/:skin", "Styling", CustomStyles, "CustomStyles"],
	["/simple-icons/:skin", "Simple icons", SimpleIcons, "SimpleIcons"],
	["/locales/:skin", "Locales", Locales, "Locales"],
	["/api/:skin", "API calls", API, "API"],
	["/extra-info/:skin", "Extra info", ExtraInfo, "ExtraInfo"],
	["/tooltips/:skin", "Tooltips", Tooltips, "Tooltips"],
	["/serverdata/:skin", "Backend data", BackendData, "BackendData"],
	["/data-provider/:skin", "Saving to backend", DataProvider, "DataProvider"],
	[
		"/serverfilter/:skin",
		"Filtering on backend",
		BackendFilter,
		"BackendFilter",
	],
];
