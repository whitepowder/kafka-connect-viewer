import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
NODE = shutil.which("node")


@pytest.mark.skipif(NODE is None, reason="node is not installed")
@pytest.mark.parametrize("script", ["frontend_smoke.cjs", "create_editor.test.cjs"])
def test_frontend_script(script):
    result = subprocess.run(
        [NODE, str(ROOT / "tests" / script), str(ROOT / "static" / "app.js")],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=ROOT,
    )
    assert result.returncode == 0, result.stdout + result.stderr
