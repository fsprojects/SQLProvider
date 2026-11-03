// fsdocs puts a member's XML doc into its tooltip as raw text, tags included.
// This renders the tags: the summary in its own colour, one line per parameter, the return value, and
// <see cref="..."/> / <paramref name="..."/> as the name they refer to.

const docTag = /<\/?(summary|param|returns|remarks|exception|typeparam|see|paramref|typeparamref|para|c|code)\b/;

function refName(el) {
    const name = el.getAttribute("cref") || el.getAttribute("name") || el.getAttribute("langword") || el.textContent;
    return name.replace(/^[A-Z]:/, "");
}

function inline(node, target) {
    for (const child of node.childNodes) {
        if (child.nodeType === Node.TEXT_NODE) {
            target.append(child.textContent.replace(/\s+/g, " "));
        } else if (child.nodeType === Node.ELEMENT_NODE) {
            const tag = child.localName;
            if (tag === "see" || tag === "seealso" || tag === "paramref" || tag === "typeparamref" || tag === "c" || tag === "code") {
                const code = document.createElement("code");
                code.textContent = child.textContent.trim() || refName(child);
                target.append(code);
            } else {
                inline(child, target);
            }
        }
    }
}

function line(cls, label, el) {
    const body = document.createDocumentFragment();
    inline(el, body);
    body.normalize();
    if (body.firstChild?.nodeType === Node.TEXT_NODE) body.firstChild.textContent = body.firstChild.textContent.trimStart();
    if (body.lastChild?.nodeType === Node.TEXT_NODE) body.lastChild.textContent = body.lastChild.textContent.trimEnd();
    const div = document.createElement("div");
    div.className = cls;
    if (label) {
        const name = document.createElement("span");
        name.className = "fsdocs-tip-doc-name";
        name.textContent = label;
        div.append(name, " ");
    }
    div.append(body);
    return div;
}

function render(text) {
    const xml = new DOMParser().parseFromString(`<doc>${text}</doc>`, "application/xml");
    if (xml.getElementsByTagName("parsererror").length > 0) return null;
    const doc = document.createElement("div");
    doc.className = "fsdocs-tip-doc";
    for (const el of xml.documentElement.children) {
        switch (el.localName) {
            case "summary": doc.append(line("fsdocs-tip-doc-summary", null, el)); break;
            case "remarks": doc.append(line("fsdocs-tip-doc-remarks", null, el)); break;
            case "param":
            case "typeparam": doc.append(line("fsdocs-tip-doc-param", el.getAttribute("name") + ":", el)); break;
            case "returns": doc.append(line("fsdocs-tip-doc-param", "Returns:", el)); break;
            case "exception": doc.append(line("fsdocs-tip-doc-param", "Throws " + refName(el) + ":", el)); break;
        }
    }
    return doc.childNodes.length > 0 ? doc : null;
}

for (const em of document.querySelectorAll("div.fsdocs-tip > em")) {
    if (!docTag.test(em.textContent)) continue;
    const doc = render(em.textContent);
    if (doc) em.replaceWith(doc);
}
