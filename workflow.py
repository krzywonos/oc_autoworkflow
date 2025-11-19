# IMPORTS
import argparse;
import json;
import luigi;
import os;
import pandas as pd;
import shutil;
import socket;
import subprocess;
import time;
import yaml;
from configparser import ConfigParser;
from multiprocessing import freeze_support;
from oc_validator.main import Validator;
from pathlib import Path;
from ruamel.yaml import YAML;
from ruamel.yaml.comments import CommentedMap;
from typing import Callable;
from urllib.parse import urlparse;
 
# CONFIG.YAML VARIABLES
input_dir = "dir/input";
temp_dir = "dir/temp";
output_dir = "dir/output";
preprocess_dir = None;
oc_virtuoso_utilities_dir = None;
oc_meta_dir = None;
oc_meta_dir_error = None;
oc_meta_val_dir = None;
oc_meta_csv_dir = None;
meta2redis_dir = None;
oc_index_config_dir = None;
oc_index_cnc_dir = None;
oc_index_dumpindex_dir = None;
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

# HELPERS
def wait_for_port(
    host: str, 
    port: int, 
    timeout: int = 60
):
    """
    Wait for a TCP port to become available.

    Parameters
    ----------
    host : str
        Hostname or IP address to connect to (e.g., "127.0.0.1").
    port : int
        TCP port number to test (e.g., 8080).
    timeout : int, optional
        Maximum number of seconds to wait before giving up (default: 60).

    Raises
    ------
    TimeoutError
        If the port is not reachable within the given timeout.

    Notes
    -----
    This function repeatedly attempts to open a socket connection to (host, port)
    every 0.5 seconds until successful or until the timeout expires.
    Useful for waiting until a service inside Docker (e.g. Fuseki, Redis)
    is ready to accept connections.
    """
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

def run(
    cmd: list[str], 
    **kwargs
):
    """
    Execute a shell command and raise an error if it fails.

    Parameters
    ----------
    cmd : list[str]
        Command and arguments as a list of strings, e.g. ["docker", "ps", "-a"].
    **kwargs :
        Additional keyword arguments passed directly to `subprocess.run()`
        (e.g., cwd, env, stdout, stderr).

    Returns
    -------
    subprocess.CompletedProcess
        The completed process result object.

    Raises
    ------
    subprocess.CalledProcessError
        If the command exits with a non-zero return code.

    Notes
    -----
    Prints the command before running it, prefixed with `$`, to provide
    a shell-like echo of what’s being executed.
    """
    print("$", " ".join(cmd));
    return subprocess.run(cmd, check=True, **kwargs);

def docker_rm(
    container: str
):
    """
    Force-remove a Docker container if it exists.

    Parameters
    ----------
    container : str
        Name or ID of the Docker container to remove.

    Notes
    -----
    Runs `docker rm -f <container>` and ignores errors if the container
    does not exist or is already stopped/removed.
    """
    try:
        run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL);
    except subprocess.CalledProcessError:
        pass;

def get_nested_yaml(
    cfg, 
    dotted, 
    default=None
):
    """
    Retrieve a value from a nested YAML dictionary using a dotted key path.

    Parameters
    ----------
    cfg : dict
        Parsed YAML configuration (as a Python dictionary).
    dotted : str
        Dotted key path, e.g. "redis.host" or "cnc.db_ra".
    default : any, optional
        Default value to return if the path does not exist.

    Returns
    -------
    any
        The retrieved value if found, or `default` if any key along the path is missing.

    Example
    -------
    >>> cfg = {"redis": {"host": "127.0.0.1", "port": 6379}}
    >>> get_nested_yaml(cfg, "redis.host")
    '127.0.0.1'
    >>> get_nested_yaml(cfg, "redis.db", default=0)
    0
    """
    cur = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def expect_in(
    cfg: dict,
    name: str,
    errors: list,
    allowed: set | None = None,
    cast: type | None = None,
    transform: Callable | None = None,
    default=None,
):
    """
    Validate and normalise a configuration entry.

    Parameters
    ----------
    cfg : dict
        Configuration dictionary (will be updated in-place if value passes checks).
    name : str
        Key to fetch from cfg.
    errors : list
        Mutable list to append error messages to.
    allowed : set, optional
        If provided, the value must be one of these.
    cast : callable, optional
        Type constructor or function to cast the value (e.g., int, float, Path).
    transform : callable, optional
        Function applied after casting (e.g., str.lower).
    default : any, optional
        Default to insert if the key is missing.

    Returns
    -------
    The validated/transformed value (or None if invalid/missing).
    """
    val = cfg.get(name, None);

    if val is None:
        if default is not None:
            cfg[name] = default;
            return default;
        errors.append(f"{name}: missing");
        return None;

    if cast is not None:
        try:
            val = cast(val);
        except Exception as e:
            errors.append(f"{name}: invalid type/value {val!r} (expected {cast.__name__}) - {e}");
            return None;

    if transform is not None:
        try:
            val = transform(val);
        except Exception as e:
            errors.append(f"{name}: transform failed ({e})");
            return None;

    if allowed is not None and val not in allowed:
        errors.append(f"{name}: {val!r} not in {sorted(allowed)}");

    cfg[name] = val;
    return val;


def clean_directory_except(
        base_dir: str | Path, 
        keep: list[str]
):
    """
    Delete everything inside `base_dir` except for specified subfolders or files.

    Parameters
    ----------
    base_dir : str | Path
        The directory whose contents will be cleaned.
    keep : list[str]
        Names (not full paths) of files or subdirectories to keep.

    Example
    -------
    >>> clean_directory_except("dir/temp", keep=["index-preprocessed", "meta-preprocessed"])
    """
    base = Path(base_dir)
    if not base.is_dir():
        raise NotADirectoryError(base)

    for entry in base.iterdir():
        if entry.name in keep:
            continue  # skip anything we want to preserve
        if entry.is_dir():
            shutil.rmtree(entry, ignore_errors=True)
        else:
            try:
                entry.unlink()
            except FileNotFoundError:
                pass

# LUIGI TASKS
class LoadConfig(luigi.Task):
    param = luigi.PathParameter(default = "dir/temp/loadconfig.txt");

    def requires(self):
        return None;

    def output(self):
        return luigi.LocalTarget(self.param);

    def run(self):
        # path to YAML
        CONFIG_PATH = Path("config.yaml");
        # load
        cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {};
        errors = [];

        validation_type = expect_in(cfg, "validation_type", errors, allowed = {0, 1, 2}, cast = int);
        validation_elimination = expect_in(cfg, "validation_elimination", errors, allowed = {"file", "line"}, transform=lambda s: str(s).lower());
        
        meta_rdf_output_in_chunks = expect_in(cfg, "meta_rdf_output_in_chunks", errors, allowed = {0, 1}, cast = int);
        meta_workers_number = expect_in(cfg, "meta_workers_number", errors, cast = int);
        meta_dir_split_number = expect_in(cfg, "meta_dir_split_number", errors, cast = int);
        meta_items_per_file = expect_in(cfg, "meta_items_per_file", errors, cast = int);
        meta_generate_rdf_files = expect_in(cfg, "meta_generate_rdf_files", errors, allowed = {0, 1}, cast = int);
        meta_zip_output_rdf = expect_in(cfg, "meta_zip_output_rdf", errors, allowed = {0, 1}, cast = int);
        meta_normalize_titles = expect_in(cfg, "meta_normalize_titles", errors, allowed = {0, 1}, cast = int);
        meta_use_doi_api_services = expect_in(cfg, "meta_use_doi_api_services", errors, allowed = {0, 1}, cast = int);

        prov_virtuoso_bulk_load = expect_in(cfg, "prov_virtuoso_bulk_load", errors, allowed = {0, 1}, cast = int);
        prov_virtuoso_dump = expect_in(cfg, "prov_virtuoso_dump", errors, allowed = {0, 1}, cast = int);
        prov_virtuoso_dump_file_limit = expect_in(cfg, "prov_virtuoso_dump_file_limit", errors, cast = str);
        prov_virtuoso_dump_compression = expect_in(cfg, "prov_virtuoso_dump_compression", errors, allowed = {0, 1}, cast = int);
        prov_virtuoso_custom = expect_in(cfg, "prov_virtuoso_custom", errors, allowed = {0, 1}, cast = int);
        prov_virtuoso_detach = expect_in(cfg, "prov_virtuoso_detach", errors, allowed = {0, 1}, cast = int);
        prov_virtuoso_wait_ready = expect_in(cfg, "prov_virtuoso_wait_ready", errors, allowed = {0, 1}, cast = int);
        prov_virtuoso_enable_write_permissions = expect_in(cfg, "prov_virtuoso_enable_write_permissions", errors, allowed = {0, 1}, cast = int);
        prov_virtuoso_force_remove = expect_in(cfg, "prov_virtuoso_force_remove", errors, allowed = {0, 1}, cast = int);

        if errors:
            raise ValueError("\n".join(errors));

        for _k, _v in cfg.items():
            globals()[_k] = _v;

        print("Loaded config keys:", ", ".join(sorted(cfg.keys())));

        with self.output().open("w") as f:
            f.write("ok\n");

class Preprocess(luigi.Task):
    param1 = luigi.PathParameter(default = "dir/temp/index-preprocessed/success.txt");
    param2 = luigi.PathParameter(default = "dir/temp/meta-preprocessed/success.txt");

    def requires(self):
        return LoadConfig();

    def output(self):
        return [
            luigi.LocalTarget(self.param1),
            luigi.LocalTarget(self.param2)
        ]

    def run(self):
        # preprocess in sparql
        if(preprocess_storage_type == "sparql"):
            try:
                # parsing the sparql endpoint
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

                # running fuseki container in docker
                docker_rm(fuseki_container);
                run(["docker", "run", "-d", "--name", fuseki_container, "-p", f"{port}:3030", fuseki_image, "--mem", f"/{dataset}"]);
                wait_for_port("localhost", port);
                print(f"Fuseki ready at http://localhost:{port}/{dataset}/sparql (requested: {preprocess_sparql_endpoint})");

                # running preprocess for meta and index
                cmd = ["python", preprocess_dir, input_dir + "/meta", temp_dir + "/meta-preprocessed", "--storage-type", "sparql", "--sparql-endpoint", preprocess_sparql_endpoint];
                run(cmd);
                print("Input for meta preprocessed");
                cmd = ["python", preprocess_dir, input_dir + "/index", temp_dir + "/index-preprocessed", "--storage-type", "sparql", "--sparql-endpoint", preprocess_sparql_endpoint];
                run(cmd);
                print("Input for index preprocessed.");
            finally:
                docker_rm(fuseki_container);
        # preprocess in redis
        elif(preprocess_storage_type == "redis"):
            try:
                # running redis in docker
                docker_rm(redis_container);
                run(["docker", "run", "-d", "--name", redis_container, "-p", f"{preprocess_redis_port}:6379", redis_image]);
                wait_for_port("localhost", preprocess_redis_port);
                print(f"Redis ready at redis://localhost:{preprocess_redis_port}");

                #running preprocess for meta and index
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
        
        for tgt in self.output():
            with tgt.open("w") as f:
                f.write("ok\n");

class Validation(luigi.Task):
    param1 = luigi.PathParameter(default = "dir/temp/index-validated/success.txt");
    param2 = luigi.PathParameter(default = "dir/temp/meta-validated/success.txt");

    def requires(self):
        return Preprocess(param1 = "dir/temp/index-preprocessed/success.txt", param2 = "dir/temp/meta-preprocessed/success.txt");

    def output(self):
        return [
            luigi.LocalTarget(self.param1),
            luigi.LocalTarget(self.param2)
        ]

    def run(self):
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
        
        for tgt in self.output():
            with tgt.open("w") as f:
                f.write("ok\n");

class DatabaseSwitchOn(luigi.Task):
    param = luigi.PathParameter(default = "dir/temp/dbswitchon.txt");

    def requires(self):
        return Validation(param1 = "dir/temp/index-validated/success.txt", param2 = "dir/temp/meta-validated/success.txt");

    def output(self):
        return luigi.LocalTarget(self.param)

    def run(self):
        # switching on REDIS for META
        docker_rm(meta_redis_container);
        run(["docker", "run", "-d", "--name", meta_redis_container, "-p", f"{meta_redis_port}:6379", redis_image]);
        wait_for_port("localhost", meta_redis_port);
        print(f"Redis ready at redis://localhost:{meta_redis_port}");

        # switching on Virtuoso in Docker for PROV
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
        
        with self.output().open("w") as f:
            f.write("ok\n");

class OCMeta(luigi.Task):
    param = luigi.PathParameter(default = "dir/output/meta/success.txt");

    def requires(self):
        return Validation(param1 = "dir/temp/index-validated/success.txt", param2 = "dir/temp/meta-validated/success.txt"), DatabaseSwitchOn(param = "dir/temp/dbswitchon.txt");

    def output(self):
        return luigi.LocalTarget(self.param);

    def run(self):
        # runtime construction of meta_config.yaml
        config_path = Path(meta_config_path);

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

        yaml_rt = YAML(typ="rt");  # roundtrip to preserve style
        yaml_rt.preserve_quotes = True;

        with Path("config.yaml").open("r", encoding="utf-8") as f:
            src = yaml_rt.load(f);

        dst = CommentedMap();
        errors = [];

        for src_path, dst_path in KEY_MAP.items():
            try:
                # get value node from src
                node = src;
                for part in src_path.split("."):
                    node = node[part];

                # create nested maps in dst and set the value node
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

        # run oc_meta
        try: 
            cmd = ["python", oc_meta_dir, "-c", os.fspath(config_path)];
            run(cmd);
        # call on_triplestore to upload triples in case of error
        except subprocess.CalledProcessError: 
            cmd = ["python", oc_meta_dir_error, meta_triplestore_url, os.fspath(meta_output_rdf_dir)];
            run(cmd);
        
        docker_rm(meta_redis_container);

        with self.output().open("w") as f:
            f.write("ok\n");

class OCMetaCsv(luigi.Task):
    param = luigi.PathParameter(default = "dir/output/meta/ocmetacsv_output/success.txt");

    def requires(self):
        return OCMeta(param = "dir/output/meta/success.txt");

    def output(self):
        return luigi.LocalTarget(self.param)

    def run(self):
        # running oc_meta_val
        cmd = ["python", oc_meta_val_dir, meta_config_path];
        run(cmd);
        
        # run redis for oc_meta_csv
        docker_rm(meta_redis_container);
        run(["docker", "run", "-d", "--name", meta_redis_container, "-p", f"{meta_redis_port}:6379", redis_image]);
        wait_for_port("localhost", meta_redis_port);
        print(f"Redis ready at redis://localhost:{meta_redis_port}");
        
        # run oc_meta_csv; have to generate rdf files during oc_meta with meta_generate_rdf_files set to 1
        cmd = ["python", oc_meta_csv_dir, "--config", meta_config_path, "--output", meta_output_dir + "/ocmetacsv_output", "--redis-host", meta_redis_host, "--redis-port", meta_redis_port, "--redis-db", meta_redis_db];
        run(cmd);

        docker_rm(meta_redis_container);

        with self.output().open("w") as f:
            f.write("ok\n");

class Meta2Redis(luigi.Task):
    param = luigi.PathParameter(default = "dir/temp/meta2redis.txt");

    def requires(self):
        return OCMetaCsv();

    def output(self):
        return luigi.LocalTarget(self.param)

    def run(self):
        # run redis in docker
        docker_rm(meta_redis_container);
        run(["docker", "run", "-d", "--name", meta_redis_container, "-p", f"{meta_redis_port}:6379", redis_image]);
        wait_for_port("localhost", meta_redis_port);
        print(f"Redis ready at redis://localhost:{meta_redis_port}");

        # run meta2redis
        cmd = ["python", meta2redis_dir, "--dump", meta_output_dir + "/ocmetacsv_output"];
        run(cmd);

        with self.output().open("w") as f:
            f.write("ok\n");

class OCIndex(luigi.Task):
    param = luigi.PathParameter(default = "dir/output/index/success.txt");

    def requires(self):
        return Meta2Redis(param = "dir/temp/meta2redis.txt");

    def output(self):
        return luigi.LocalTarget(self.param);

    def run(self):
        # runtime generation of config.ini
        YAML_IN  = Path("config.yaml");
        INI_OUT  = Path(oc_index_config_dir);
        cfg = yaml.safe_load(YAML_IN.read_text(encoding="utf-8")) or {};
        ini = ConfigParser(interpolation=None, delimiters=("="));
        ini.optionxform = str;

        # mapping config.yaml to config.ini
        KEY_MAP = {
            # IDENTIFIER 
            "index_identifier_pmid" : ("identifier", "pmid"),
            "index_identifier_doi" : ("identifier", "doi"),
            "index_identifier_omid" : ("identifier", "omid"),

            # LOGGING 
            "index_logging_verbose": ("logging", "verbose"),

            # REDIS
            "index_redis_host": ("redis", "host"),
            "index_redis_port": ("redis", "port"),
            "index_redis_batch_size": ("redis", "batch_size"),

            # CNC
            "index_cnc_orcid": ("cnc", "orcid"),
            "index_cnc_lookup": ("cnc", "lookup"),
            "index_cnc_use_api": ("cnc", "use_api"),
            "index_cnc_services": ("cnc", "services"),
            "index_cnc_identifiers": ("cnc", "identifiers"),
            "index_cnc_br_ids": ("cnc", "br_ids"),
            "index_cnc_ra_ids": ("cnc", "ra_ids"),
            "index_cnc_db_cits": ("cnc", "db_cits"),
            "index_cnc_db_omid": ("cnc", "db_omid"),
            "index_cnc_db_br": ("cnc", "db_br"),
            "index_cnc_db_ra": ("cnc", "db_ra"),

            # CNC SERVICE TEMPLATE
            "index_cnc_service_template_prefix": ("CNC_SERVICE_TEMPLATE", "prefix"),
            "index_cnc_service_template_parser": ("CNC_SERVICE_TEMPLATE", "parser"),
            "index_cnc_service_template_source": ("CNC_SERVICE_TEMPLATE", "source"),
            "index_cnc_service_template_agent": ("CNC_SERVICE_TEMPLATE", "agent"),
            "index_cnc_service_template_baseurl": ("CNC_SERVICE_TEMPLATE", "baseurl"),
            "index_cnc_service_template_idbaseurl": ("CNC_SERVICE_TEMPLATE", "idbaseurl"),
            "index_cnc_service_template_service": ("CNC_SERVICE_TEMPLATE", "service"),
            "index_cnc_service_template_datasource": ("CNC_SERVICE_TEMPLATE", "datasource"),
            "index_cnc_service_template_identifier": ("CNC_SERVICE_TEMPLATE", "identifier"),

            # INDEX
            "index_index_prefix": ("INDEX", "prefix"),
            "index_index_parser": ("INDEX", "parser"),
            "index_index_validator": ("INDEX", "validator"),
            "index_index_source": ("INDEX", "source"),
            "index_index_agent": ("INDEX", "agent"),
            "index_index_baseurl": ("INDEX", "baseurl"),
            "index_index_idbaseurl": ("INDEX", "idbaseurl"),
            "index_index_service": ("INDEX", "service"),
            "index_index_datasource": ("INDEX", "datasource"),
            "index_index_db": ("INDEX", "db"),
            "index_index_identifier": ("INDEX", "identifier")
        }

        # application of mapped overrides
        errors = []
        for yaml_path, (section, key) in KEY_MAP.items():
            val = get_nested_yaml(cfg, yaml_path, default=None)
            if val is None:
                continue
            ini.setdefault(section, {})
            ini[section][key] = str(val)

        # write config.ini
        INI_OUT.parent.mkdir(parents=True, exist_ok=True)
        with INI_OUT.open("w", encoding="utf-8") as f:
            ini.write(f, space_around_delimiters=False)

        # run cnc.py
        cmd = ["python", oc_index_cnc_dir, "--input", input_dir + "/index", "--intype", "CSV", "--service", index_service, "--output", output_dir + "/index", "--processes", str(index_cnc_processes)];
        run(cmd);

        # run dump_index.py
        cmd = ["python", oc_index_dumpindex_dir, "--date", index_date, "--workers", str(index_dumpindex_workers)];
        run(cmd);

        with self.output().open("w") as f:
            f.write("ok\n");

class Dump(luigi.Task):
    param = luigi.PathParameter(default = "dir/output/n-quads-dump/success.txt");

    def requires(self):
        return OCIndex(param = "dir/output/index/success.txt");

    def output(self):
        return luigi.LocalTarget(self.param)

    def run(self):
        # run virtuoso_utilities/dump_quadstore.py to get PROV dump
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

        with self.output().open("w") as f:
            f.write("ok\n");

class CleanUp(luigi.Task):
    param = luigi.PathParameter("cleanup.txt");

    def requires(self):
        return DatabaseSwitchOn(param = "dir/temp/dbswitchon.txt");

    def output(self):
        return luigi.LocalTarget(self.param)

    def run(self):
        # turn off redis
        docker_rm(meta_redis_container);
        # turn off virtuoso
        docker_rm(prov_virtuoso_name);

        # delete temp_dir
        shutil.rmtree(temp_dir);

        # delete unnecessary file in output_dir
        clean_directory_except(
            output_dir,
            keep = ["n-quads-dump", "ocmetacsv_output", "rdf"] #TODO: add index output here
        );

        # delete unnecessary runtime files from the main folder
        shutil.rmtree("storage");
        Path("failed_queries.txt").unlink(missing_ok=True);
        Path("gently_run.bat").unlink(missing_ok=True);
        Path("gently_stop.bat").unlink(missing_ok=True);
        Path("meta_br.csv").unlink(missing_ok=True);
        Path("meta_ra.csv").unlink(missing_ok=True);
        Path("ts_upload_cache.json").unlink(missing_ok=True);

        # delete Virtuoso data?
        # shutil.rmtree(virtuoso-data);

        with self.output().open("w") as f:
            f.write("ok\n");

# MAIN
if __name__ == "__main__":
    freeze_support();

    parser = argparse.ArgumentParser(add_help=True);
    parser.add_argument(
        "--local-scheduler",
        action="store_true",
        help="Use Luigi's local in-process scheduler instead of a central luigid.",
    );
    args, _unknown = parser.parse_known_args();

    tasks_to_run = [
        LoadConfig(),
        Preprocess(),
        Validation(),
        DatabaseSwitchOn(),
        OCMeta(),
        OCMetaCsv(),
        Meta2Redis(),
        #OCIndex(),
        #Dump(),
        #CleanUp()
    ];

    ok = luigi.build(
        tasks_to_run,
        workers = 1,
        local_scheduler = bool(args.local_scheduler),
        scheduler_host = "127.0.0.1",
        scheduler_port = 8082,
        detailed_summary = True
    );
    raise SystemExit(0 if ok else 1);