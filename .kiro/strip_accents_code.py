import json, io, unicodedata

NB = "notebook/proyecto_ocean_watch.ipynb"
with io.open(NB, "r", encoding="utf-8") as f:
    nb = json.load(f)

# Map only Spanish accented letters and inverted marks. Do NOT touch other unicode.
TRANS = {
    "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n",
    "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ü": "U", "Ñ": "N",
    "¿": "", "¡": "",
}

changed = 0
for c in nb["cells"]:
    if c["cell_type"] != "code":
        continue
    new_src = []
    for line in c["source"]:
        nl = "".join(TRANS.get(ch, ch) for ch in line)
        if nl != line:
            changed += 1
        new_src.append(nl)
    c["source"] = new_src

with io.open(NB, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

# Verify none remain in code cells
remaining = []
for i, c in enumerate(nb["cells"]):
    if c["cell_type"] == "code":
        for line in c["source"]:
            for ch in line:
                if ch in "áéíóúüñÁÉÍÓÚÜÑ¿¡":
                    remaining.append((i, repr(ch)))
print("lineas modificadas:", changed)
print("acentos restantes en codigo:", remaining if remaining else "NONE")
