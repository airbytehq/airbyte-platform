import json
from typing import Dict


def extract_engine_versions(package_json_path: str) -> Dict:
    with open(package_json_path) as file:
        package_data: Dict = json.load(file)
    
    engines = package_data.get("engines", {})
    node_version = engines.get("node")
    pnpm_version = engines.get("pnpm")
    
    return {"node": node_version, "pnpm": pnpm_version}
