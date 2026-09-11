<script setup>
import { Tooltip } from "@svar-ui/vue-core";
import { getID } from "@svar-ui/lib-dom";

defineOptions({ name: "FilemanagerTooltip", inheritAttrs: false });

const props = defineProps({
  api: {},
  at: { default: "point" },
  content: {},
  overflow: { type: Boolean, default: false },
  resolver: { type: Function },
});

function defaultResolver(element) {
  if (!props.api) return null;

  if (element.classList.contains("wx-item")) {
    const id = getID(element);
    if (!id) return null;
    const file = props.api.getFile(id);
    if (!file) return null;
    if (props.overflow) {
      const nameEl = element.querySelector(".wx-name");
      const startEl = nameEl?.firstElementChild;
      if (!startEl || startEl.scrollWidth <= startEl.clientWidth) {
        return null;
      }
    }
    if (props.content) {
      return { api: props.api, data: { file } };
    } else {
      return file.name;
    }
  }

  return null;
}

const resolverFn = props.resolver || defaultResolver;
</script>

<template>
  <Tooltip
    :at="at"
    :content="content"
    :resolver="resolverFn"
    v-bind="$attrs"
  >
    <slot />
  </Tooltip>
</template>
