# imports
import luigi;
import time;
import subprocess;
import argparse;
import socket;
import time;
import subprocess;
import yaml;
import os;
import requests;
import pandas as pd;
import shutil;
import json;
import webbrowser;
from pathlib import Path;
from urllib.parse import urlparse;
from oc_validator.main import Validator;
from ruamel.yaml import YAML;
from ruamel.yaml.comments import CommentedMap;

# declaration of variables loaded from config.yaml
input_dir = None;
temp_dir = None;
output_dir = None;
preprocess_dir = None;
oc_validator_dir = None;
oc_virtuoso_utilities_dir = None;
oc_meta_dir = None;
oc_meta_dir_error = None;
oc_meta_val_dir = None;
oc_meta_csv_dir = None;
meta2redis_dir = None;
oc_index_cnc_dir = None;
oc_index_dumpindex_dir = None;
upload_dir = None;
publication_dir = None;
oc_index_config_dir = None;
fuseki_image = None;
redis_image = None;
blazegraph_image = None;
redis_container = None;
fuseki_container = None;
blazegraph_container = None;
preprocess_storage_type = None;
preprocess_redis_port = None;
preprocess_redis_db_number = None;
preprocess_sparql_endpoint = None;
validation_type = None;
validation_elimination = None;
meta_config_path = None;
meta_triplestore_url = None;
meta_provenance_triplestore_url = None;
meta_base_iri = None;
meta_context_path = None;
meta_resp_agent = None;
meta_source = None;
meta_cache_endpoint = None;
meta_cache_update_endpoint = None;
meta_graphdb_connector_name = None;
meta_output_dir = None;
meta_redis_container = None;
meta_redis_host = None;
meta_redis_port = None;
meta_redis_db = None;
meta_redis_cache_db = None;
meta_supplier_prefix = None;
meta_rdf_output_in_chunks = None;
meta_workers_number = None;
meta_dir_split_number = None;
meta_items_per_file = None;
meta_default_dir = None;
meta_generate_rdf_files = None;
meta_zip_output_rdf = None;
meta_output_rdf_dir = None;
meta_silencer = None;
meta_normalize_titles = None;
meta_use_doi_api_services = None;
prov_virtuoso_bulk_load = None;
prov_virtuoso_bulk_load_dir = None;
prov_virtuoso_dump = None;
prov_virtuoso_dump_dir = None;
prov_virtuoso_dump_file_limit = None;
prov_virtuoso_dump_compression = None;
prov_virtuoso_custom = None;
prov_virtuoso_name = None;
prov_virtuoso_http_port = None;
prov_virtuoso_isql_port = None;
prov_virtuoso_data_dir = None;
prov_virtuoso_dba_username = None;
prov_virtuoso_dba_password = None;
prov_virtuoso_mount_volume = None;
prov_virtuoso_network = None;
prov_virtuoso_memory = None;
prov_virtuoso_detach = None;
prov_virtuoso_wait_ready = None;
prov_virtuoso_enable_write_permissions = None;
prov_virtuoso_force_remove = None;
index_service = None;
index_cnc_processes = None;
index_date = None;
index_dumpindex_workers = None;

# helper functions
def wait_for_port(host: str, port: int, timeout: int = 60):
    # waits for opening the specified port
    timeout = 60;
    deadline = time.time() + timeout;
    last_error = None;
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return;
        except OSError as e:
            last_error = e;
            time.sleep(0.5);
    raise TimeoutError(f"Port {host}:{port} not ready after {timeout}s; last error: {last_error}");

def run(cmd: list[str], **kwargs):
    # run a command in terminal
    print("$", " ".join(cmd));
    return subprocess.run(cmd, check=True, **kwargs);

def docker_rm(container: str):
    # turn off container with the specified name in docker
    try:
        run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL);
    except subprocess.CalledProcessError:
        pass;

# luigi tasks

class LoadConfig(luigi.Task):
    param = luigi.Parameter(default = 42);

    def run(self):
        # path to YAML
        CONFIG_PATH = Path("config.yaml");

        # load
        cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {};

        errors = [];

        def _expect_in(name, allowed=None, cast=None, transform=None):
            val = cfg.get(name, None);
            if val is None:
                errors.append(f"{name}: missing");
                return None;
            if cast:
                try:
                    val = cast(val);
                except Exception:
                    errors.append(f"{name}: invalid type/value {val!r} (expected {cast.__name__})");
                    return None;
            if transform:
                val = transform(val);
            if allowed and val not in allowed:
                errors.append(f"{name}: {val!r} not in {sorted(allowed)}");
            cfg[name] = val;
            return val;

        validation_type = _expect_in("validation_type", allowed = {0, 1, 2}, cast = int);
        validation_elimination = _expect_in("validation_elimination", allowed = {"file", "line"}, transform=lambda s: str(s).lower());
        
        meta_rdf_output_in_chunks = _expect_in("meta_rdf_output_in_chunks", allowed = {0, 1}, cast = int);
        meta_workers_number = _expect_in("meta_workers_number", cast = int);
        meta_dir_split_number = _expect_in("meta_dir_split_number", cast = int);
        meta_items_per_file = _expect_in("meta_items_per_file", cast = int);
        meta_generate_rdf_files = _expect_in("meta_generate_rdf_files", allowed = {0, 1}, cast = int);
        meta_zip_output_rdf = _expect_in("meta_zip_output_rdf", allowed = {0, 1}, cast = int);
        meta_normalize_titles = _expect_in("meta_normalize_titles", allowed = {0, 1}, cast = int);
        meta_use_doi_api_services = _expect_in("meta_use_doi_api_services", allowed = {0, 1}, cast = int);

        prov_virtuoso_bulk_load = _expect_in("prov_virtuoso_bulk_load", allowed = {0, 1}, cast = int);
        prov_virtuoso_dump = _expect_in("prov_virtuoso_dump", allowed = {0, 1}, cast = int);
        prov_virtuoso_dump_file_limit = _expect_in("prov_virtuoso_dump_file_limit", cast = str);
        prov_virtuoso_dump_compression = _expect_in("prov_virtuoso_dump_compression", allowed = {0, 1}, cast = int);
        prov_virtuoso_custom = _expect_in("prov_virtuoso_custom", allowed = {0, 1}, cast = int);
        prov_virtuoso_detach = _expect_in("prov_virtuoso_detach", allowed = {0, 1}, cast = int);
        prov_virtuoso_wait_ready = _expect_in("prov_virtuoso_wait_ready", allowed = {0, 1}, cast = int);
        prov_virtuoso_enable_write_permissions = _expect_in("prov_virtuoso_enable_write_permissions", allowed = {0, 1}, cast = int);
        prov_virtuoso_force_remove = _expect_in("prov_virtuoso_force_remove", allowed = {0, 1}, cast = int);

        # raise combined error if any
        if errors:
            raise ValueError("\n".join(errors));

        # assign all keys from YAML to same-named variables (module scope)
        # (safe because we validated critical ones above)
        for _k, _v in cfg.items():
            globals()[_k] = _v;

        # show a quick summary
        print("Loaded config keys:", ", ".join(sorted(cfg.keys())));

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param);

class Preprocess(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return LoadConfig(self.param);

    def run(self):

        print("Running task Preprocess");
        print("Placeholder - call preprocess with all files in input_dir and store output in temp_dir");

        if(preprocess_storage_type == "sparql"):
            try:
                u = urlparse(preprocess_sparql_endpoint);
                host = u.hostname or "localhost";
                port = u.port or (443 if u.scheme == "https" else 80);

                parts = [p for p in (u.path or "").split("/") if p];
                if not parts:
                    raise ValueError(f"Cannot determine dataset from endpoint path: {preprocess_sparql_endpoint}");
                
                if parts[-1].lower() in ("sparql", "query"):
                    if len(parts) < 2:
                        raise ValueError(f"Endpoint path too short to infer dataset: {preprocess_sparql_endpoint}");
                    dataset = parts[-2];
                else:
                    dataset = parts[-1];

                if host not in ("localhost", "127.0.0.1"): 
                    print(f"Endpoint host is '{host}'. This script exposes Fuseki on the local machine; \n please access it via http://localhost:{port}/{dataset}/sparql");

                docker_rm(fuseki_container);
                run(["docker", "run", "-d", "--name", fuseki_container, "-p", f"{port}:3030", fuseki_image, "--mem", f"/{dataset}"]);
                wait_for_port("localhost", port);
                print(f"Fuseki ready at http://localhost:{port}/{dataset}/sparql (requested: {preprocess_sparql_endpoint})");

                cmd = ["python", preprocess_dir, input_dir + "/meta", temp_dir + "/meta-preprocessed", "--storage-type", "sparql", "--sparql-endpoint", preprocess_sparql_endpoint];
                run(cmd);
                print("Input for meta preprocessed");
                cmd = ["python", preprocess_dir, input_dir + "/index", temp_dir + "/index-preprocessed", "--storage-type", "sparql", "--sparql-endpoint", preprocess_sparql_endpoint];
                run(cmd);
                print("Input for index preprocessed.");
            finally:
                docker_rm(fuseki_container);
        
        elif(preprocess_storage_type == "redis"):
            try:
                docker_rm(redis_container);
                run(["docker", "run", "-d", "--name", redis_container, "-p", f"{preprocess_redis_port}:6379", redis_image]);
                wait_for_port("localhost", preprocess_redis_port);
                print(f"Redis ready at redis://localhost:{preprocess_redis_port}");

                cmd = ["python", preprocess_dir, input_dir + "/meta", temp_dir + "/meta-preprocessed", "--storage-type", "redis", "--redis-db", preprocess_redis_db_number];
                run(cmd);
                print("Input for meta preprocessed");
                cmd = ["python", preprocess_dir, input_dir + "/index", temp_dir + "/index-preprocessed", "--storage-type", "redis", "--redis-db", preprocess_redis_db_number];
                run(cmd);
                print("Input for index preprocessed.");
            finally:
                docker_rm(redis_container);
        
        else:
            print("Incorrect value for preprocess_storage_type");

        print("Finished task Preprocess");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param);
    
class Validation(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Preprocess(self.param);

    def run(self):
        print("Running task Validation");

        # prune id data from index-preprocessed
        CSV_DIR = Path("dir/temp/index-preprocessed");
        COLUMNS_TO_KEEP = ["citing", "cited"];
        RENAME_MAP = {"citing": "citing_id", "cited": "cited_id"};

        for csv_file in CSV_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file);
            keep = [c for c in COLUMNS_TO_KEEP if c in df.columns];
            if not keep:
                continue;

            df = df[keep].rename(columns=RENAME_MAP);
            df.to_csv(csv_file, index=False);

        # validation of meta
        meta_in = Path("dir/temp/meta-preprocessed");
        meta_val_dir = Path(temp_dir) / "meta-validated" / "validation";
        meta_out_dir = Path(temp_dir) / "meta-validated";
        meta_val_dir.mkdir(parents=True, exist_ok=True);
        meta_map = {};

        counter = 0;
        for file in sorted(meta_in.iterdir()):
            if not (file.is_file() and file.suffix.lower() == ".csv"):
                continue;

            # snapshot "before"
            before = set(sorted(meta_val_dir.glob("out_validate_meta*.json")));

            # run validator
            if validation_type == 0:
                v = Validator(str(file), str(meta_val_dir));
            elif validation_type == 1:
                v = Validator(str(file), str(meta_val_dir), use_meta_endpoint=True);
            elif validation_type == 2:
                v = Validator(str(file), str(meta_val_dir), verify_id_existence=False);
            else:
                raise ValueError(f"Unknown validation_type={validation_type}");
            v.validate();

            time.sleep(0.05);

            # detect the JSON that appeared
            after = sorted(meta_val_dir.glob("out_validate_meta*.json"));
            new_jsons = [p for p in after if p not in before];
            json_path = max(new_jsons, key=lambda p: p.stat().st_mtime) if new_jsons else None;

            # parse bad rows from JSON
            bad_rows = set();
            if json_path and json_path.exists():
                try:
                    doc = json.loads(json_path.read_text(encoding="utf-8"));
                    if isinstance(doc, list):
                        for entry in doc:
                            pos = entry.get("position") or {};
                            table = pos.get("table");
                            if isinstance(table, dict):
                                for row_key in table.keys():
                                    try:
                                        bad_rows.add(int(row_key));
                                    except (ValueError, TypeError):
                                        pass;
                except Exception as e:
                    print(f"Could not parse {json_path}: {e}");


            meta_map[file] = {"json": json_path, "bad_rows": bad_rows};
            counter += 1;
            print(f"Validated META file no. {counter}: {file.name} → "
                f"JSON: {json_path.name if json_path else '??'}, bad rows: {len(bad_rows)}");

        # validation of index
        index_in = Path("dir/temp/index-preprocessed");
        index_val_dir = Path(temp_dir) / "index-validated" / "validation";
        index_out_dir = Path(temp_dir) / "index-validated";
        index_val_dir.mkdir(parents=True, exist_ok=True);
        index_map = {};

        counter = 0;
        for file in sorted(index_in.iterdir()):
            if not (file.is_file() and file.suffix.lower() == ".csv"):
                continue;

            before = set(sorted(index_val_dir.glob("out_validate_cits*.json")));

            if validation_type == 0:
                v = Validator(str(file), str(index_val_dir));
            elif validation_type == 1:
                v = Validator(str(file), str(index_val_dir), use_meta_endpoint=True);
            elif validation_type == 2:
                v = Validator(str(file), str(index_val_dir), verify_id_existence=False);
            else:
                raise ValueError(f"Unknown validation_type={validation_type}");
            v.validate();

            time.sleep(0.05);

            after = sorted(index_val_dir.glob("out_validate_cits*.json"));
            new_jsons = [p for p in after if p not in before];
            json_path = max(new_jsons, key=lambda p: p.stat().st_mtime) if new_jsons else None;

            # parse bad rows from JSON
            bad_rows = set();
            if json_path and json_path.exists():
                try:
                    doc = json.loads(json_path.read_text(encoding="utf-8"));
                    if isinstance(doc, list):
                        for entry in doc:
                            pos = entry.get("position") or {};
                            table = pos.get("table");
                            if isinstance(table, dict):
                                for row_key in table.keys():
                                    try:
                                        bad_rows.add(int(row_key));
                                    except (ValueError, TypeError):
                                        pass;
                except Exception as e:
                    print(f"Could not parse {json_path}: {e}");


            index_map[file] = {"json": json_path, "bad_rows": bad_rows};
            counter += 1;
            print(f"Validated INDEX file no. {counter}: {file.name} → "
                f"JSON: {json_path.name if json_path else '??'}, bad rows: {len(bad_rows)}");

        # elimination of incorrect entries
        if validation_elimination == "file":
            print("Validation elimination mode: FILE");

            # META
            meta_out_dir.mkdir(parents=True, exist_ok=True);
            for csv_path, info in meta_map.items():
                if info["bad_rows"]:
                    print(f"Skipping META file (has errors): {csv_path.name}");
                else:
                    shutil.copyfile(csv_path, meta_out_dir / csv_path.name);
                    print(f"Copied META file: {csv_path.name}");

            # INDEX
            index_out_dir.mkdir(parents=True, exist_ok=True);
            for csv_path, info in index_map.items():
                if info["bad_rows"]:
                    print(f"Skipping INDEX file (has errors): {csv_path.name}");
                else:
                    shutil.copyfile(csv_path, index_out_dir / csv_path.name);
                    print(f"Copied INDEX file: {csv_path.name}");

        elif validation_elimination == "line":
            print("Validation elimination mode: LINE");

            # META
            meta_out_dir.mkdir(parents=True, exist_ok=True);
            for csv_path, info in meta_map.items():
                df = pd.read_csv(csv_path);
                if info["bad_rows"]:
                    df = df.drop(index=[r for r in info["bad_rows"] if 0 <= r < len(df)]);
                    print(f"Wrote META filtered CSV: {csv_path.name} (removed {len(info['bad_rows'])} lines)");
                else:
                    print(f"Copied META CSV without changes: {csv_path.name}");
                df.to_csv(meta_out_dir / csv_path.name, index=False)

            # INDEX
            index_out_dir.mkdir(parents=True, exist_ok=True);
            for csv_path, info in index_map.items():
                df = pd.read_csv(csv_path);
                if info["bad_rows"]:
                    df = df.drop(index=[r for r in info["bad_rows"] if 0 <= r < len(df)]);
                    print(f"Wrote INDEX filtered CSV: {csv_path.name} (removed {len(info['bad_rows'])} lines)");
                else:
                    print(f"Copied INDEX CSV without changes: {csv_path.name}");
                df.to_csv(index_out_dir / csv_path.name, index=False);

        else:
            print(f"Validation elimination mode: NONE (validation_elimination={validation_elimination!r})");

        print("Finished task Validation");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)

class DatabaseSwitchOn(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Preprocess(self.param);

    def run(self):
        print("Running task DatabaseSwitchOn");
        
        # # triplestore for META (ask Arcangelo which one)
        # u = urlparse(meta_triplestore_url);
        # host = u.hostname or "127.0.0.1";
        # port = u.port or (443 if u.scheme == "https" else 80);

        # docker_rm(blazegraph_container);

        # run([
        #     "docker", "run", "-d",
        #     "--name", blazegraph_container,
        #     "-p", f"{port}:8080",
        #     blazegraph_image
        # ]);

        # wait_for_port(host, port);

        # # 1
        # NAMESPACE = "kb"

        # meta = urlparse(meta_triplestore_url)
        # base_root = f"{meta.scheme}://{host}:{port}"

        # if "/bigdata" in meta_triplestore_url:
        #     base_path = "/bigdata"
        # elif "/blazegraph" in meta_triplestore_url:
        #     base_path = "/blazegraph"
        # else:
        #     base_path = "/bigdata"

        # ns_base   = f"{base_root}{base_path}/namespace/{NAMESPACE}"
        # sparql_url = f"{ns_base}/sparql"
        # textindex_url = f"{ns_base}/textIndex"

        # deadline = time.time() + 60 
        # ready = False
        # last_err = None
        # headers_probe = {"Accept": "*/*"}

        # while time.time() < deadline:
        #     try:
        #         resp = requests.get(sparql_url, headers=headers_probe, timeout=5)
        #         if 200 <= resp.status_code < 500:
        #             ready = True
        #             break
        #     except Exception as e:
        #         last_err = e
        #     time.sleep(1)

        # if not ready:
        #     raise RuntimeError(
        #         f"Namespace SPARQL not reachable at {sparql_url} after waiting. "
        #         f"Check base path ({base_path}) and namespace ({NAMESPACE}). Last error: {last_err}"
        #     )

        # uris = [
        #     "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue",
        # ]
        # data = [("uri", u) for u in uris] + [("force-index-create", "true")]

        # r = requests.post(textindex_url, data=data, timeout=120)
        # r.raise_for_status()
        # print(f"Text index created/rebuilt via {textindex_url}: {r.status_code}")
        # # 2

        # print("Text index created/rebuilt:", r.status_code)

        # print("Blazegraph is up.");
        # print("Workbench UI:", f"{u.scheme}://{host}:{port}/blazegraph");
        # print("SPARQL endpoint:", f"{u.scheme}://{host}:{port}/blazegraph/sparql");

        # endpoint = "http://127.0.0.1:8805/bigdata/namespace/kb/sparql"
        # construct_query = """\
        # CONSTRUCT { ?s ?p ?o }
        # WHERE { ?s ?p ?o }
        # LIMIT 1
        # """

        # r = requests.post(
        #     endpoint,
        #     data=construct_query.encode("utf-8"),
        #     headers={
        #         "Content-Type": "application/sparql-query",  # crucial
        #         "Accept": "application/rdf+xml",             # or "text/turtle"
        #     },
        #     timeout=60,
        # )
        # print(r.status_code, r.headers.get("Content-Type"))
        # print(r.text[:400])
        # r.raise_for_status()

        # REDIS for META
        docker_rm(meta_redis_container);
        run(["docker", "run", "-d", "--name", meta_redis_container, "-p", f"{meta_redis_port}:6379", redis_image]);
        wait_for_port("localhost", meta_redis_port);
        print(f"Redis ready at redis://localhost:{meta_redis_port}");


        # QLEVER in Docker for INDEX???



        # Virtuoso in Docker for PROV
        cmd = ["python", oc_virtuoso_utilities_dir + "/launch_virtuoso.py"];
        if prov_virtuoso_custom == 1:
            if prov_virtuoso_name != "":
                cmd.append("--name");
                cmd.append(prov_virtuoso_name);
            if prov_virtuoso_http_port != "":
                cmd.append("--http-port");
                cmd.append(prov_virtuoso_http_port);
            if prov_virtuoso_isql_port != "":
                cmd.append("--isql-port");
                cmd.append(prov_virtuoso_isql_port);
            if prov_virtuoso_data_dir != "":
                cmd.append("--data-dir");
                cmd.append(prov_virtuoso_data_dir);
            if prov_virtuoso_dba_password != "":
                cmd.append("--dba-password");
                cmd.append(prov_virtuoso_dba_password);
            if prov_virtuoso_mount_volume.strip():
                for word in prov_virtuoso_mount_volume.split():
                    cmd.append("--mount-volume")
                    cmd.append(word)                
            if prov_virtuoso_network != "":
                cmd.append("--network");
                cmd.append(prov_virtuoso_network);
            if prov_virtuoso_memory != "":
                cmd.append("--memory");
                cmd.append(prov_virtuoso_memory);
            if prov_virtuoso_detach == 1:
                cmd.append("--detach");
            if prov_virtuoso_wait_ready == 1:
                cmd.append("--wait-ready");
            if prov_virtuoso_enable_write_permissions == 1:
                cmd.append("--enable-write-permissions");
            if prov_virtuoso_force_remove == 1:
                cmd.append("--force-remove");
        run(cmd);

        # virtuoso_utilities/rebuild_fulltext_index.py to rebuild Virtuoso text index
        cmd = ["python", oc_virtuoso_utilities_dir + "/rebuild_fulltext_index.py"];
        cmd.append("--password");
        if prov_virtuoso_dba_password != "":
            cmd.append(prov_virtuoso_dba_password);
        else:
            cmd.append("dba");
        if prov_virtuoso_custom == 1:
            cmd.append("--port");
            cmd.append(prov_virtuoso_isql_port);
            cmd.append("--user");
            cmd.append(prov_virtuoso_dba_username);
        cmd.append("--docker-container");
        cmd.append(prov_virtuoso_name);
        run(cmd);

        # virtuoso_utilities/bulk_load.py n-quads to populate PROV if enabled
        if prov_virtuoso_bulk_load and prov_virtuoso_custom:
            cmd = ["python", oc_virtuoso_utilities_dir + "/bulk_load.py"];
            cmd.append("--data-directory");
            cmd.append(prov_virtuoso_bulk_load_dir);
            cmd.append("--password");
            if prov_virtuoso_dba_password != "":
                cmd.append(prov_virtuoso_dba_password);
            else:
                cmd.append("dba");
            if prov_virtuoso_custom == 1:
                if prov_virtuoso_name != "":
                    cmd.append("--docker-container");
                    cmd.append(prov_virtuoso_name);
                if prov_virtuoso_isql_port != "":
                    cmd.append("--port");
                    cmd.append(prov_virtuoso_isql_port);
                if prov_virtuoso_dba_username != "":
                    cmd.append("--user");
                    cmd.append(prov_virtuoso_dba_username);
            cmd.append("--recursive");
            run(cmd);
        
        print("Finished task DatabaseSwitchOn");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)

class OCMeta(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Validator(self.param), DatabaseSwitchOn(self.param);

    def run(self):
        print("Running task OCMeta");
        
        config_path = Path(meta_config_path);

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
            "meta_use_doi_api_services": "use_doi_api_service",
            "meta_provenance_endpoints": "provenance_endpoints",
            "meta_input_csv_dir": "input_csv_dir",
            "meta_base_output_dir": "base_output_dir",
            "meta_virtuoso_full_text_search": "virtuoso_full_text_search",
            "meta_blazegraph_full_text_search": "blazegraph_full_text_search",
            "meta_fuseki_full_text_search": "fuseki_full_text_search"
        };

        yaml_rt = YAML(typ="rt");  # round-trip to preserve quotes/styles
        yaml_rt.preserve_quotes = True;

        with Path("config.yaml").open("r", encoding="utf-8") as f:
            src = yaml_rt.load(f);

        dst = CommentedMap();
        errors = [];

        for src_path, dst_path in KEY_MAP.items():
            try:
                # --- get value node from src (preserve style by reusing the node) ---
                node = src;
                for part in src_path.split("."):
                    node = node[part];

                # --- create nested maps in dst and set the value node ---
                cur = dst;
                parts = dst_path.split(".");
                for p in parts[:-1]:
                    if p not in cur or not isinstance(cur[p], CommentedMap):
                        cur[p] = CommentedMap();
                    cur = cur[p];
                cur[parts[-1]] = node;
            except Exception as e:
                errors.append(f"Missing/invalid path '{src_path}': {e}");

        if errors:
            raise ValueError("\n".join(errors));

        yaml_out = YAML(typ="rt");
        yaml_out.preserve_quotes = True;
        yaml_out.default_flow_style = False;
        yaml_out.indent(mapping=2, sequence=2, offset=2);

        with config_path.open("w", encoding="utf-8") as f:
            yaml_out.dump(dst, f);

        print(f"Wrote {config_path.resolve()}");

        try: # run oc_meta
            cmd = ["python", oc_meta_dir, "-c", os.fspath(config_path)];
            run(cmd);
        except subprocess.CalledProcessError: # call on_triplestore to upload triples in case of error
            cmd = ["python", oc_meta_dir_error, meta_triplestore_url, os.fspath(meta_output_rdf_dir)];
            run(cmd);
        
        docker_rm(meta_redis_container);
        print("Finished task OCMeta");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)
    
class OCMetaVal(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return OCMeta(self.param);

    def run(self):
        print("Running task OCMetaVal");
        
        cmd = ["python", oc_meta_val_dir, meta_output_dir + "/csv", meta_config_path];
        run(cmd);

        print("Finished task OCMetaVal");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)

class OCMetaCsv(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return OCMetaVal(self.param);

    def run(self):
        print("Running task OCMetaCsv");
        
        docker_rm(meta_redis_container);
        run(["docker", "run", "-d", "--name", meta_redis_container, "-p", f"{meta_redis_port}:6379", redis_image]);
        wait_for_port("localhost", meta_redis_port);
        print(f"Redis ready at redis://localhost:{meta_redis_port}");
        
        #have to generate rdf files during oc_meta with meta_generate_rdf_files set to 1
        cmd = ["python", oc_meta_csv_dir, "--config", meta_config_path, "--output", meta_output_dir + "/ocmetacsv_output", "--redis-host", meta_redis_host, "--redis-port", meta_redis_port, "--redis-db", meta_redis_db];
        run(cmd);

        docker_rm(meta_redis_container);

        print("Finished task OCMetaCsv");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)
    
class Meta2Redis(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return OCMetaCsv(self.param);

    def run(self):
        print("Running task Meta2Redis");
        
        #TODO turn on in-RAM REDIS?
        docker_rm(meta_redis_container);
        run(["docker", "run", "-d", "--name", meta_redis_container, "-p", f"{meta_redis_port}:6379", redis_image]);
        wait_for_port("localhost", meta_redis_port);
        print(f"Redis ready at redis://localhost:{meta_redis_port}");

        cmd = ["python", meta2redis_dir, "--dump", meta_output_dir + "/ocmetacsv_output"];
        run(cmd);

        print("Finished task Meta2Redis");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)

class OCIndex(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Meta2Redis(self.param);

    def run(self):
        print("Running task OCIndex");
        
        #TODO call oc_index to read data from citations input file and in-RAM REDIS to update PROV and create raw data
        print("Placeholder - call oc_index to read data from citations input file and in-RAM REDIS to update PROV and create raw data");

        #cnc.py
        cmd = ["python", oc_index_cnc_dir, "-i", input_dir + "/index", "-t", "CSV", "--service", index_service, "-o", output_dir + "/index", "-p", str(index_cnc_processes)];
        run(cmd);

        #dump_index.py
        cmd = ["python", oc_index_dumpindex_dir, "-d", index_date, "-w", str(index_dumpindex_workers)];
        run(cmd);
        
        print("Finished task OCIndex");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)

class Upload(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return OCIndex(self.param);

    def run(self):
        print("Running task Upload");
        
        #TODO call upload to use raw data to update INDEX and PROV(?)
        print("Placeholder - call upload to use raw data to update INDEX and PROV?");
        
        #virtuoso_utilities/dump_quadstore.py to get PROV dump
        if prov_virtuoso_dump and prov_virtuoso_custom:
            cmd = ["python", oc_virtuoso_utilities_dir + "/dump_quadstore.py"];
            cmd.append("--password")
            if prov_virtuoso_dba_password != "":
                cmd.append(prov_virtuoso_dba_password);
            else:
                cmd.append("dba");
            cmd.append("--output-dir");
            cmd.append(prov_virtuoso_dump_dir);
            if prov_virtuoso_custom == 1:
                cmd.append("--port");
                cmd.append(prov_virtuoso_isql_port);
                cmd.append("--user");
                cmd.append(prov_virtuoso_dba_username);
            cmd.append("--docker-container");
            cmd.append(prov_virtuoso_name);
            cmd.append("--file-length-limit");
            cmd.append(prov_virtuoso_dump_file_limit);
            if not prov_virtuoso_dump_compression:
                cmd.append("--no-compression");
            run(cmd);

        print("Finished task Upload");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)
    
class Publication(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Upload(self.param);

    def run(self):
        print("Running task Publication");
        
        #TODO? ?maybe? ?call? ?publication? ?with? ?raw? ?data?
        print("Placeholder? - calling publication with raw data?");
        
        print("Finished task Publication");

    def output(self):
        return luigi.LocalTarget("abcabc-%s.txt" % self.param)

class CleanUp(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return DatabaseSwitchOn(self.param);

    def run(self):
        print("Running task DatabaseSwitchOff");
        
        # turning blazegraph off
        #docker_rm(blazegraph_container);
        # turn off redis
        print("Placeholder - turn off Redis here")
        # turn off virtuoso
        docker_rm(prov_virtuoso_name);
        #TODO - turn off QLEVER or whatever is used for index
        print("Placeholder - turn off QLEVER here");

        # delete temp_dir
        #shutil.rmtree(temp_dir);

        print("Finished task CleanUp");

if __name__ == "__main__":

    task_loadconfig = LoadConfig();
    task_preprocess = Preprocess();
    task_validation = Validation();
    task_dbswitchon = DatabaseSwitchOn();
    task_ocmeta = OCMeta();
    task_ocmetaval = OCMetaVal();
    task_ocmetacsv = OCMetaCsv();
    task_meta2redis = Meta2Redis();
    task_ocindex = OCIndex();
    task_upload = Upload();
    task_publication = Publication();
    task_cleanup = CleanUp();

    #webbrowser.open("https://localhost:8082");

    start = time.time();
    print("");
    task_loadconfig.run();
    #task_preprocess.run();
    #task_validation.run();
    #task_dbswitchon.run();
    #task_ocmeta.run();
    #task_ocmetaval.run();
    #task_ocmetacsv.run();
    task_meta2redis.run();
    task_ocindex.run();
    #task_upload.run();
    #task_publication.run();
    #task_cleanup.run();
    end = time.time();
    print("Total runtime: " + str(end-start) + "s");