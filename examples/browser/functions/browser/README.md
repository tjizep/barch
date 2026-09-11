<div align="center">

# SVAR Vue File Manager | File Explorer

[![npm](https://img.shields.io/npm/v/@svar-ui/vue-filemanager.svg)](https://www.npmjs.com/package/@svar-ui/vue-filemanager)
[![License](https://img.shields.io/github/license/svar-widgets/vue-filemanager)](https://github.com/svar-widgets/vue-filemanager/blob/main/license.txt)
[![npm downloads](https://img.shields.io/npm/dm/@svar-ui/vue-filemanager.svg)](https://www.npmjs.com/package/@svar-ui/vue-filemanager)

</div>

<div align="center">

[Homepage](https://svar.dev/vue/filemanager/) • [Getting Started](https://docs.svar.dev/vue/filemanager/getting_started/) • [Demos](https://docs.svar.dev/vue/filemanager/samples/)

</div>

[SVAR Vue File Manager](https://svar.dev/vue/filemanager/) is a flexible file explorer component for Vue 3 apps. It offers a familiar interface for browsing, organizing, and previewing files. Integrate this file management component with any backend, whether you're using local storage, databases, or cloud services.

<div align="center">
  <img src="https://cdn.svar.dev/public/file-manager-1400.png" alt="SVAR File Manager for Vue - UI" width="700">
</div>
<br>

### :sparkles: Key features:

-   Basic file operations: create, delete, copy, rename, cut, paste
-   Download and upload files
-   Files tree view
-   List and tiles views
-   File preview pane with file information (file size, type, modified date, etc)
-   Split view to manage files between different locations
-   Built-in search box
-   Context menu and toolbar
-   Keyboard navigation
-   Used storage info
-   Full TypeScript support

[See the live demo](https://svar.dev/demos/vue/filemanager/) to try all thees features in action.

### :hammer_and_wrench: How to Use

To install SVAR Vue File Manager:

```
npm install @svar-ui/vue-filemanager
```

To use the widget, simply import the package and include the component in your Vue file:

```vue
<script setup>
import { Filemanager } from "@svar-ui/vue-filemanager";
import "@svar-ui/vue-filemanager/all.css";

// files and folders
const data = [
    {
        id: "/Code",
        date: new Date(2023, 11, 2, 17, 25),
        type: "folder",
    },
    {
        id: "/Code/Comments.svelte",
        date: new Date(2023, 11, 2, 18, 48),
        type: "file",
        size: 682566,
    }
];
// storage usage info
const drive = {
    used: 15200000000,
    total: 50000000000,
};
</script>

<template>
  <Filemanager :data="data" :drive="drive" />
</template>
```

For further instructions, see the detailed [how-to-start guide](https://docs.svar.dev/vue/filemanager/getting_started/).

### :computer: How to Modify

Typically, you don't need to modify the code. However, if you wish to do so, follow these steps:

1. Run `yarn` to install dependencies. Note that this project is a monorepo using `yarn` workspaces, so npm will not work
2. Start the project in development mode with `yarn start`

### :white_check_mark: Run Tests

To run the test:

1. Start the test examples with:
    ```sh
    yarn start:tests
    ```
2. In a separate console, run the end-to-end tests with:
    ```sh
    yarn test:cypress
    ```
### ⭐ Show Your Support

If SVAR Vue File Manager helps your project, [give it a star](https://github.com/svar-widgets/vue-filemanager). It helps other developers discover this library and motivates us to keep supporting and improving the component.

### :speech_balloon: Need Help?

[Post an Issue](https://github.com/svar-widgets/vue-filemanager/issues/) or use our [community forum](https://forum.svar.dev).
