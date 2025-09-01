import re

RE_DUE = re.compile(r'(?:(?:до|by)\s*(\d{4}-\d{2}-\d{2}))', re.I)

def parse_freeform(text: str):
    priority = "P2"
    for p in ("P0", "P1", "P2", "P3"):
        if p in text:
            priority = p
            text = text.replace(p, "")
            break
    m_due = RE_DUE.search(text)
    due = m_due.group(1) if m_due else None

    # labels (#one #two)
    labels = []
    if "#" in text:
        labels = [s.strip().lstrip("#") for s in re.split(r"[,\s]+", " ".join([w for w in text.split() if w.startswith('#')])) if s.strip()]
        for l in labels:
            text = text.replace(f"#{l}", "")

    # assignee @user
    assignee = None
    for token in text.split():
        if token.startswith('@'):
            assignee = token
            text = text.replace(token, "")
            break

    title = re.sub(r"\s+", " ", text).strip()
    # project is first label if exists
    project = labels[0] if labels else None
    return title, priority, assignee or "", due, labels, project
