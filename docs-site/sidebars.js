// @ts-check

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.

 @type {import('@docusaurus/plugin-content-docs').SidebarsConfig}
 */
const sidebars = {
  // By default, Docusaurus generates a sidebar from the docs folder structure
  tutorialSidebar: [
    {
      type: 'doc',
      id: 'intro',
      label: 'Introduction',
    },
    {
      type: 'doc',
      id: 'getting-started',
      label: 'Getting Started',
    },
    {
      type: 'category',
      label: 'Usage',
      items: [
        { type: 'doc', id: 'usage', label: 'Overview' },
        'scenarios/S01_Provision_new_stream',
        'scenarios/S02_Open_stream_for_writing',
        'scenarios/S04_Write_to_stream',
        'scenarios/S05_Read_from_stream',
        'scenarios/S06_Include_additional_entities',
        'scenarios/S07_Custom_stream_metadata',
        'scenarios/Virtual_partitions',
        'scenarios/S08_Concurrency_conflicts',
        'scenarios/S09_Handling_duplicates',
        'scenarios/S10_Stream_directory',
        'scenarios/S11_Sharding_streams',
      ],
    },
    {
      type: 'doc',
      id: 'design',
      label: 'Design',
    },
    {
      type: 'doc',
      id: 'limitations',
      label: 'Limitations',
    },
    {
      type: 'doc',
      id: 'api',
      label: 'API Reference',
    },
  ],

  // But you can create a sidebar manually
  /*
  tutorialSidebar: [
    'intro',
    'hello',
    {
      type: 'category',
      label: 'Tutorial',
      items: ['tutorial-basics/create-a-document'],
    },
  ],
   */
};

export default sidebars;
