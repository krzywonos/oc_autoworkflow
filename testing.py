# pip install ruamel.yaml
from pathlib import Path
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

meta_config_path = "dir/temp/meta_config.yaml"
SRC_PATH = Path("config.yaml")     # <- your source YAML
DST_PATH = Path(meta_config_path)    # <- file to create

# Map: "source.path.with.dots" -> "dest.path.with.dots"
# Example: copy src["build"]["code"] -> dst["app"]["version_code"]
KEY_MAP = {
    "meta_triplestore_url": "triplestore_url",
    "meta_provenance_triplestore_url": "provenance_triplestore_url",
    "meta_base_iri": "base_iri",
    "meta_context_path": "context_path",
    "meta_resp_agent": "resp_agent",
    "meta_source": "source",
    "meta_cache_endpoint": "cache_endpoint",
    "meta_cache_update_endpoint": "cache_update_endpoint",
    "meta_graphdb_connector_name": "graphdb_connector_name",
    "meta_output_dir": "base_output_dir",
    "meta_supplier_prefix": "supplier_prefix",
    "meta_rdf_output_in_chunks": "rdf_output_in_chunks",
    "meta_workers_number": "workers_number",
    "meta_dir_split_number": "dir_split_number",
    "meta_items_per_file": "items_per_file",
    "meta_default_dir": "default_dir",
    "meta_generate_rdf_files": "generate_rdf_files",
    "meta_zip_output_rdf": "zip_output_rdf",
    "meta_output_rdf_dir": "output_rdf_dir",
    "meta_silencer": "silencer",
    "meta_normalize_titles": "normalize_titles",
    "meta_use_doi_api_services": "use_doi_api_services",
    "meta_provenance_endpoints": "provenance_endpoints",
    "meta_input_csv_dir": "input_csv_dir",
    "meta_base_output_dir": "base_output_dir",
    "meta_virtuoso_full_text_search": "virtuoso_full_text_search",
    "meta_blazegraph_full_text_search": "blazegraph_full_text_search",
    "meta_fuseki_full_text_search": "fuseki_full_text_search"
}

yaml_rt = YAML(typ="rt")  # round-trip to preserve quotes/styles
yaml_rt.preserve_quotes = True

with SRC_PATH.open("r", encoding="utf-8") as f:
    src = yaml_rt.load(f)

dst = CommentedMap()
errors = []

for src_path, dst_path in KEY_MAP.items():
    try:
        # --- get value node from src (preserve style by reusing the node) ---
        node = src
        for part in src_path.split("."):
            node = node[part]

        # --- create nested maps in dst and set the value node ---
        cur = dst
        parts = dst_path.split(".")
        for p in parts[:-1]:
            if p not in cur or not isinstance(cur[p], CommentedMap):
                cur[p] = CommentedMap()
            cur = cur[p]
        cur[parts[-1]] = node
    except Exception as e:
        errors.append(f"Missing/invalid path '{src_path}': {e}")

if errors:
    raise ValueError("\n".join(errors))

yaml_out = YAML(typ="rt")
yaml_out.preserve_quotes = True
yaml_out.default_flow_style = False
yaml_out.indent(mapping=2, sequence=2, offset=2)

with DST_PATH.open("w", encoding="utf-8") as f:
    yaml_out.dump(dst, f)

print(f"Wrote {DST_PATH.resolve()}")