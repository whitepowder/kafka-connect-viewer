import re
import struct
import zlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
CSS = (STATIC / "styles.css").read_text(encoding="utf-8")
APP = (STATIC / "app.js").read_text(encoding="utf-8")
COLOR = re.compile(r"#[0-9a-fA-F]{3,8}\b|rgba?\([^)]*\)|\b(?:white|black)\b(?!-)")

LIGHT_PALETTE = {
    "--bg": "#efeae2",
    "--ink": "#1d1a16",
    "--muted": "#6f675e",
    "--line": "#ddd4c8",
    "--panel": "#fbf8f3",
    "--sidebar": "#221f1b",
    "--accent": "#c2410c",
    "--accent-soft": "#fff1e8",
    "--ok": "#176b3a",
    "--fail": "#9f2d2d",
    "--neutral-bg": "#eeeae4",
    "--surface": "#fff",
}


def _block(selector: str) -> dict[str, str]:
    match = re.search(re.escape(selector) + r"\s*\{(.*?)\n\}", CSS, re.S)
    assert match, f"{selector} block is missing"
    return dict(re.findall(r"(--[\w-]+):\s*([^;]+);", match.group(1)))


def _luminance(hex_color: str) -> float:
    value = hex_color.lstrip("#")
    if len(value) == 3:
        value = "".join(char * 2 for char in value)
    channels = [int(value[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    linear = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in channels]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def _contrast(a: str, b: str) -> float:
    high, low = sorted((_luminance(a), _luminance(b)), reverse=True)
    return (high + 0.05) / (low + 0.05)


def test_light_palette_is_unchanged():
    light = _block(":root")
    for name, value in LIGHT_PALETTE.items():
        assert light[name] == value, name
    assert "color-scheme: light" in re.search(r":root\s*\{(.*?)\n\}", CSS, re.S).group(1)


def test_dark_palette_defines_every_light_variable():
    light = {name for name in _block(":root") if name not in {"--sans", "--mono"}}
    dark = _block(':root[data-theme="dark"]')
    assert set(dark) == light
    assert 'color-scheme: dark' in re.search(r':root\[data-theme="dark"\]\s*\{(.*?)\n\}', CSS, re.S).group(1)


def test_dark_palette_is_dark_brown_and_readable():
    dark = _block(':root[data-theme="dark"]')
    for name in ("--bg", "--panel", "--surface", "--sidebar"):
        red, green, blue = (int(dark[name][i:i + 2], 16) for i in (1, 3, 5))
        assert _luminance(dark[name]) < 0.03, name
        assert red >= green >= blue, f"{name} is a warm brown/charcoal"
    for text, background in [("--ink", "--bg"), ("--ink", "--panel"), ("--ink", "--surface"), ("--muted", "--panel"),
                             ("--sidebar-ink", "--sidebar"), ("--on-ink", "--ink"), ("--fail", "--fail-bg"),
                             ("--ok", "--ok-bg"), ("--pause", "--pause-bg"), ("--badge-ink", "--badge-bg")]:
        assert _contrast(dark[text], dark[background]) >= 4.5, f"{text} on {background}"
    assert _contrast(dark["--accent"], dark["--bg"]) >= 3


def test_dark_sidebar_is_warm_brown_and_main_panel_near_black():
    dark = _block(':root[data-theme="dark"]')
    assert dark["--sidebar"] == "#241e1b"
    assert dark["--bg"] == dark["--panel"] == "#171413"
    assert _luminance(dark["--sidebar"]) > _luminance(dark["--panel"]), "the sidebar is lighter than the main panel"
    assert _contrast(dark["--sidebar-muted"], dark["--sidebar"]) >= 4.5
    assert _contrast(dark["--line"], dark["--panel"]) >= 1.4, "pane borders stay visible"
    for raised in ("--surface", "--row-hover", "--neutral-bg"):
        assert _luminance(dark[raised]) > _luminance(dark["--panel"]), f"{raised} stands out from the main panel"


def test_no_hardcoded_colors_outside_the_palettes():
    body = re.sub(r":root(?:\[data-theme=\"dark\"\])?\s*\{.*?\n\}", "", CSS, flags=re.S)
    body = re.sub(r'url\("data:[^"]*"\)', "", body)
    leftovers = []
    for line in body.splitlines():
        for match in COLOR.finditer(line):
            if match.group(0).startswith("rgba(255, 255, 255") and ("cluster" in line or "background: rgba" in line):
                continue
            if match.group(0) == "rgba(255,255,255,.08)" and "--surface-2" in line:
                continue
            leftovers.append(line.strip())
    assert leftovers == []


def _rule(selector: str) -> str:
    start = CSS.index("\n" + selector) + 1
    return CSS[start:CSS.index("}", start)]


def test_graph_and_editors_use_theme_variables():
    for selector in (
        ".gnode rect", ".gnode-topic > rect", ".gedge {", ".gedge.is-dlq", ".gnode-badge rect", ".legend-line {",
        ".create-curl {", "textarea {", ".trace {", ".btn.primary {", ".toast {", ".modal {", ".dialog {",
    ):
        assert "var(--" in _rule(selector), selector


def test_mascot_is_pixelated_and_responsive():
    rule = CSS[CSS.index(".brand-mascot img {"):]
    rule = rule[:rule.index("}")]
    assert "image-rendering: pixelated" in rule
    assert "height: auto" in rule and "width: 100%" in rule
    assert re.search(r"\.brand-mascot \{[^}]*width: min\(100%, \d+px\)", CSS)
    narrow = CSS[CSS.index("@media (max-width: 980px)"):]
    assert ".brand-mascot { width:" in narrow


def test_mascot_has_no_plate_outline_or_filter():
    box = _rule(".brand-mascot {")
    image = _rule(".brand-mascot img {")
    for prop in ("background", "border", "border-radius", "box-shadow", "filter", "outline"):
        assert not re.search(rf"(^|\s){prop}\s*:", box + image), f"no {prop} around the mascot"
    assert "mascot-outline" not in CSS and "mascot-plate" not in CSS


def _png_alpha_columns(path: Path) -> tuple[int, int, list[int]]:
    """Max alpha per column of an 8-bit, non-interlaced RGBA PNG."""
    data = path.read_bytes()
    width, height = struct.unpack(">II", data[16:24])
    idat, pos = b"", 8
    while pos < len(data):
        (size,), kind = struct.unpack(">I", data[pos:pos + 4]), data[pos + 4:pos + 8]
        if kind == b"IDAT":
            idat += data[pos + 8:pos + 8 + size]
        pos += 12 + size
    raw, stride = zlib.decompress(idat), width * 4
    prev, columns = bytearray(stride), [0] * width
    for y in range(height):
        kind, line = raw[y * (stride + 1)], bytearray(raw[y * (stride + 1) + 1:(y + 1) * (stride + 1)])
        for x in range(stride):
            left, up, corner = (line[x - 4] if x >= 4 else 0), prev[x], (prev[x - 4] if x >= 4 else 0)
            if kind == 1:
                line[x] = (line[x] + left) & 255
            elif kind == 2:
                line[x] = (line[x] + up) & 255
            elif kind == 3:
                line[x] = (line[x] + (left + up) // 2) & 255
            elif kind == 4:
                p = left + up - corner
                pa, pb, pc = abs(p - left), abs(p - up), abs(p - corner)
                line[x] = (line[x] + (left if pa <= pb and pa <= pc else up if pb <= pc else corner)) & 255
        for x, alpha in enumerate(line[3::4]):
            columns[x] = max(columns[x], alpha)
        prev = line
    return width, height, columns


def test_mascots_share_one_canvas_and_the_crop_keeps_the_whole_snake_and_lantern():
    image = _rule(".brand-mascot img {")
    ratio_w, ratio_h = map(int, re.search(r"aspect-ratio: (\d+) / (\d+);", image).groups())
    position = float(re.search(r"object-position: ([\d.]+)% 50%;", image).group(1)) / 100
    assert "object-fit: cover" in image
    sizes = set()
    for name in ("light", "dark"):
        path = STATIC / "assets" / f"kcv-snake-{name}.png"
        assert path.read_bytes()[24:26] == b"\x08\x06", "8-bit RGBA"
        width, height, columns = _png_alpha_columns(path)
        sizes.add((width, height))
        window = ratio_w * height / ratio_h
        left = position * (width - window)
        visible = [x for x, alpha in enumerate(columns) if alpha >= 4]
        assert left <= visible[0] and visible[-1] < left + window, f"{name} art fits inside the crop window"
        assert visible[0] - left >= 4 and left + window - visible[-1] >= 4, f"{name} keeps a margin on both sides"
    assert sizes == {(1964, 400)}, "both themes use the same canvas, so the snake keeps its size and position"
    assert 'width: "1964"' in APP and 'height: "400"' in APP


def test_readme_branding_is_theme_aware_and_points_at_real_assets():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    picture = readme[:readme.index("# KCV")]
    assert '<source media="(prefers-color-scheme: dark)" srcset="static/assets/kcv-snake-dark.png">' in picture
    assert '<source media="(prefers-color-scheme: light)" srcset="static/assets/kcv-snake-light.png">' in picture
    assert re.search(r'<img src="static/assets/kcv-snake-light.png" alt="[^"]+" width="300">', picture)
    assert '<p align="center">' in picture
    for path in re.findall(r'(?:srcset|src)="([^"]+)"', picture):
        assert (ROOT / path).is_file(), path


def test_index_applies_the_theme_before_styles_and_app():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    boot = html.index('src="/static/theme-boot.js')
    assert boot < html.index('href="/static/styles.css') < html.index('src="/static/app.js')
    assert '<meta name="color-scheme" content="light dark">' in html
    assert "<script>" not in html, "inline scripts are blocked by the CSP"


def test_theme_files_are_served_under_the_existing_csp():
    with TestClient(app) as client:
        for path, kind in [
            ("/static/assets/kcv-snake-light.png", "image/png"),
            ("/static/assets/kcv-snake-dark.png", "image/png"),
            ("/static/theme-boot.js", "javascript"),
        ]:
            response = client.get(path)
            assert response.status_code == 200, path
            assert kind in response.headers["content-type"]
        csp = client.get("/").headers["content-security-policy"]
    assert "script-src 'self'" in csp
    assert "img-src 'self' data:" in csp
