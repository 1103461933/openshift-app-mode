
from pathlib import Path
import yaml
import re

source = Path("../operator-rendered-cluster.yaml")
output_dir = Path("operator-resources")
output_dir.mkdir(exist_ok=True)

def safe_name(value):
    return re.sub(r"[^a-zA-Z0-9_.-]", "-", str(value))

# Read the file and detect its encoding (UTF-8 or UTF-16)
raw_content = source.read_bytes()

if raw_content.startswith((b"\xff\xfe", b"\xfe\xff")):
    content = raw_content.decode("utf-16")
else:
    content = raw_content.decode("utf-8-sig")

# Split YAML documents
for index, resource in enumerate(yaml.safe_load_all(content), start=1):
    if not isinstance(resource, dict):
        continue

    kind = resource.get("kind", "unknown")
    metadata = resource.get("metadata") or {}
    name = metadata.get("name", f"resource-{index}")
    namespace = metadata.get("namespace", "cluster")

    filename = (
        f"{index:03d}-{safe_name(namespace)}-"
        f"{safe_name(kind)}-{safe_name(name)}.yaml"
    )

    destination = output_dir / filename

    with destination.open("w", encoding="utf-8") as out:
        yaml.safe_dump(
            resource,
            out,
            sort_keys=False,
            allow_unicode=True
        )

    print(f"Generated: {destination}")
