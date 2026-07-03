"""Guard against the vendored proto drifting from the canonical engine proto."""

from pathlib import Path


def test_vendored_proto_matches_canonical():
    here = Path(__file__).resolve()
    vendored = here.parents[1] / "proto" / "akidb.proto"
    canonical = here.parents[3] / "crates" / "proto" / "proto" / "akidb.proto"
    if not canonical.exists():
        return  # canonical not present (standalone published package) — skip
    assert vendored.read_text() == canonical.read_text(), (
        "sdks/python/proto/akidb.proto has drifted from the canonical proto; "
        "re-vendor it and regenerate stubs (see README)."
    )
