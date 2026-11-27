// Syntax-check every inline <script> in the site pages. A single stray quote
// in a tooltip string breaks a whole page's JavaScript silently in the
// browser; this fails loudly instead.
const fs = require('fs');
const vm = require('vm');
let bad = 0;
for (const page of ['index.html', 'grid.html', 'europe.html']) {
  const html = fs.readFileSync(page, 'utf8');
  const re = /<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi;
  let m, n = 0;
  while ((m = re.exec(html))) {
    n++;
    try { new vm.Script(m[1], { filename: `${page}#script${n}` }); }
    catch (e) {
      bad++;
      const line = html.slice(0, m.index).split('\n').length + (e.stack.match(/:(\d+)/) ? +e.stack.match(/:(\d+)/)[1] - 1 : 0);
      console.error(`${page} inline script ${n} (~line ${line}): ${e.message}`);
    }
  }
  console.log(`${page}: ${n} inline script(s) checked`);
}
process.exit(bad ? 1 : 0);
