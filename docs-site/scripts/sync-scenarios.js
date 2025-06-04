const fs = require('fs');
const path = require('path');

const scenarioDirs = [
  path.resolve(__dirname, '../../Source/Example/Scenarios'),
  path.resolve(__dirname, '../../Source/Streamstone.Tests/Scenarios'),
];
const docsDir = path.resolve(__dirname, '../docs/scenarios');

function findCsFile(mdFile) {
  const base = path.basename(mdFile, '.md');
  for (const dir of scenarioDirs) {
    const csPath = path.join(dir, base + '.cs');
    if (fs.existsSync(csPath)) return csPath;
  }
  return null;
}

function updateMarkdown(mdPath, csPath) {
  const md = fs.readFileSync(mdPath, 'utf8');
  const cs = fs.readFileSync(csPath, 'utf8');
  const codeBlockRegex = /```csharp[\s\S]*?```/g;
  const newBlock = `\n\`\`\`csharp title="${path.basename(csPath)}"\n${cs.trim()}\n\`\`\``;
  const updated = md.replace(codeBlockRegex, newBlock);
  if (md !== updated) {
    fs.writeFileSync(mdPath, updated, 'utf8');
    console.log(`Updated: ${path.basename(mdPath)}`);
  }
}

fs.readdirSync(docsDir)
  .filter(f => f.endsWith('.md'))
  .forEach(mdFile => {
    const mdPath = path.join(docsDir, mdFile);
    const csPath = findCsFile(mdFile);
    if (csPath) updateMarkdown(mdPath, csPath);
    else console.warn(`No .cs file found for ${mdFile}`);
  }); 