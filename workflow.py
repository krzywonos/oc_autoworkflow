import luigi;
import time;
import subprocess;
import argparse;
import socket;
import time;
import subprocess;
import yaml;
from pathlib import Path;
from urllib.parse import urlparse;
from oc_validator.main import Validator;


INPUT_DIR = "dir/input";
TEMP_DIR = "dir/temp";
OUTPUT_DIR = "dir/output";
PREPROCESS_DIR = "../oc_meta/oc_meta/run/meta/preprocess_input.py";
OC_VALIDATOR_DIR = "./oc_validator/oc_validator/main.py";
OC_VIRTUOSO_UTILITIES_DIR = "./virtuoso_utilities/virtuoso_utilities";
OC_META_DIR = "./oc_meta/oc_meta/run/meta_process.py";
OC_META_VAL_DIR = "./oc_meta/oc_meta/run/meta/check_results.py";
OC_META_CSV = "./oc_meta/oc_meta/run/csv_generator_lite.py";
META2REDIS_DIR = "index/scripts/ocworkflow.py/populate_redis()";
OC_INDEX_DIR = "index/scripts/ocworkflow.py/gen_zipbatch()";
UPLOAD_DIR = "";
PUBLICATION_DIR = "";

FUSEKI_IMAGE = "stain/jena-fuseki"
REDIS_IMAGE = "redis:7-alpine"
REDIS_CONTAINER = "my-redis"
FUSEKI_CONTAINER = "my-fuseki"

# preprocess_input default values
PREPROCESS_STORAGE_TYPE = "sparql" # can be "redis" or "sparql"
PREPROCESS_REDIS_DB_NUMBER = "10";
PREPROCESS_SPARQL_ENDPOINT = "localhost:3030/ds/sparql";

# validation values
VALIDATION_TYPE = "2"; # 0 - basic validation, 1 - validation with META endpoint, 2 - skipping ID existence checks

# values for SPARQL database for META
META_TRIPLESTORE_URL = "http://127.0.0.1:8805/sparql"; # Endpoint URL to load the output RDF
META_PROVENANCE_TRIPLESTORE_URL = "http://127.0.0.1:8806/sparql"; #TODO this should always be virtuoso no?
META_BASE_IRI = "https://w3id.org/oc/meta/"; # The base URI of entities on Meta. This setting can be safely left as is
META_CONTEXT_PATH = "https://w3id.org/oc/corpus/context.json"; # URL where the namespaces and prefixes used in the OpenCitations Data Model are defined. This setting can be safely left as is
META_RESP_AGENT = "https://w3id.org/oc/meta/prov/pa/1"; # A URI string representing the provenance agent which is considered responsible for the RDF graph manipulation
META_SOURCE = "https://api.crossref.org/"; # Data source URL. This setting can be safely left as is
META_OUTPUT_DIR = OUTPUT_DIR + "/meta";
META_REDIS_HOST = "localhost"; #
META_REDIS_PORT = 6379; #
META_REDIS_DB = 0; #
META_REDIS_CACHE_DB = 1; #
META_SUPPLIER_PREFIX = "060"; # A prefix for the sequential number in entities’ URIs. This setting can be safely left as is
META_RDF_OUTPUT_IN_CHUNKS = 0; # If True, save all the graphset and provset in one file, and save all the graphset on the triplestore. 
META_WORKERS_NUMBER = 16; # Number of cores to devote to the Meta process
META_DIR_SPLIT_NUMBER = 10000; # Number of files per folder. dir_split_number's value must be multiple of items_per_file's value. This parameter is useful only if you choose to return the output in json-ld format
META_ITEMS_PER_FILE = 1000; # Number of items per file. This parameter is useful only if you choose to return the output in json-ld format
META_DEFAULT_DIR = "_"; # This value is used as the default prefix if no prefix is specified. It is a deprecated parameter, valid only for backward compatibility and can safely be ignored
META_GENERATE_RDF_FILES = 0; # If True, generate and store the RDF files during the meta process. If False, RDF files will not be generated.
META_ZIP_OUTPUT_RDF = 1; # If True, the folder specified in output_rdf_dir must contain zipped JSON files, and the output will be zipped 
META_OUTPUT_RDF_DIR = META_OUTPUT_DIR + "/rdf"; # Folder where RDF files are saved. Since these files are the heaviest, it is sometimes convenient to save them on HDD, while the triplestore needs to be on SSD for its efficient operation
META_SILENCER = '["author", "editor", "publisher"]'; # Fields in the silencer list are only updated if there is no information on that field in OpenCitations Meta. For example, if 'author' is specified, any new authors are not added to the list if authors are already present.
META_NORMALIZE_TITLES = 1; #
META_USE_DOI_API_SERVICE = 0; # If True, use the DOI API service to check if DOIs are valid

# values for QLEVER database in Docker for INDEX


# values for Virtuoso in Docker for PROV
PROV_VIRTUOSO_BULK_LOAD = 1; # default: 0. set to 1 to enable bulk loading n-quads to Virtuoso
PROV_VIRTUOSO_BULK_LOAD_DIR = "dir/input/virtuoso"; # directory containing n-quads to populate PROV in Virtuoso with n-quads. MUST BE ACCESSIBLE BY VIRTUOSO
PROV_VIRTUOSO_DUMP = 1; #default: 0. set to 1 to enable quadstore dumping of PROV from Virtuoso
PROV_VIRTUOSO_DUMP_DIR = "dir/output/n-quads-dump"; # directory for n-quad dump containing PROV from Virtuoso 
PROV_VIRTUOSO_DUMP_FILE_LIMIT = 100000000; #maximum length of dump files in bytes
PROV_VIRTUOSO_DUMP_COMPRESSION = 1; # default: 1. set to 0 to disable gzip compression
PROV_VIRTUOSO_CUSTOM = 1; # default: 1. set to 0 to disable customised usage. 
PROV_VIRTUOSO_NAME = "virtuoso"; # please consult virtuoso_utilities' README.md for usage and default values
PROV_VIRTUOSO_HTTP_PORT = "8888"; # please consult virtuoso_utilities' README.md for usage and default values
PROV_VIRTUOSO_ISQL_PORT = "1111"; # please consult virtuoso_utilities' README.md for usage and default values
PROV_VIRTUOSO_DATA_DIR = ""; # please consult virtuoso_utilities' README.md for usage and default values
PROV_VIRTUOSO_DBA_USERNAME = "dba"; # please consult virtuoso_utilities' README.md for usage and default values
PROV_VIRTUOSO_DBA_PASSWORD = "dba"; # please consult virtuoso_utilities' README.md for usage and default values
PROV_VIRTUOSO_MOUNT_VOLUME = ""; # please consult virtuoso_utilities' README.md for usage and default values
PROV_VIRTUOSO_NETWORK = ""; # please consult virtuoso_utilities' README.md for usage and default values
PROV_VIRTUOSO_MEMORY = "16g"; # defaults to 2/3 of host memory with psutil installed, otherwise 2g. 
PROV_VIRTUOSO_DETACH = 1; # default: 1. Run container in detached mode.
PROV_VIRTUOSO_WAIT_READY = 1; # default: 1. Wait until Virtuoso is ready to accept connections.
PROV_VIRTUOSO_ENABLE_WRITE_PERMISSIONS = 1; # default: 1. Makes database publicly writable.

# helpers

def wait_for_port(host: str, port: int, timeout: int = 60):
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
    print("$", " ".join(cmd));
    return subprocess.run(cmd, check=True, **kwargs);

def docker_rm(container: str):
    try:
        run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL);
    except subprocess.CalledProcessError:
        pass;  # already gone

def virtuoso_rebuild_index():
    cmd = ["python", OC_VIRTUOSO_UTILITIES_DIR + "/rebuild_fulltext_index.py"];
    if PROV_VIRTUOSO_DBA_PASSWORD != "":
        cmd.append(PROV_VIRTUOSO_DBA_PASSWORD);
    else:
        cmd.append("dba");
    if PROV_VIRTUOSO_CUSTOM == 1:
        cmd.append("--port");
        cmd.append(PROV_VIRTUOSO_ISQL_PORT);
        cmd.append("--user");
        cmd.append(PROV_VIRTUOSO_DBA_USERNAME);
    cmd.append("--docker-container");
    cmd.append(PROV_VIRTUOSO_NAME);
    run(cmd);

# redis

def start_redis(container=REDIS_CONTAINER, port=6379):
    docker_rm(container);
    run(["docker", "run", "-d", "--name", container, "-p", f"{port}:6379", REDIS_IMAGE]);
    wait_for_port("localhost", port);
    print(f"Redis ready at redis://localhost:{port}");

# fuseki

def parse_fuseki_from_endpoint(endpoint: str):
    """
    Extract host, dataset, port from a SPARQL endpoint.
    Example: http://localhost:3030/ds/sparql -> host = localhost, dataset = ds, port = 3030
    """
    u = urlparse(endpoint);
    host = u.hostname or "localhost";
    port = u.port or (443 if u.scheme == "https" else 80);

    # path parts without empties
    parts = [p for p in (u.path or "").split("/") if p];
    if not parts:
        raise ValueError(f"Cannot determine dataset from endpoint path: {endpoint}");
    # common patterns end with 'sparql' or 'query'
    if parts[-1].lower() in ("sparql", "query"):
        if len(parts) < 2:
            raise ValueError(f"Endpoint path too short to infer dataset: {endpoint}");
        dataset = parts[-2];
    else:
        dataset = parts[-1];

    return host, dataset, port;

def start_fuseki_for_endpoint(endpoint: str, container=FUSEKI_CONTAINER):
    """
    Start an in-memory Fuseki so that the query endpoint will be at exactly the
    dataset+port implied by `endpoint`. (We publish the container's 3030 to the
    host port parsed from the URL, and create an in-memory dataset with that name.)
    """
    host, dataset, port = parse_fuseki_from_endpoint(endpoint)

    if host not in ("localhost", "127.0.0.1"): 
        print(f"Endpoint host is '{host}'. This script exposes Fuseki on the local machine; \n please access it via http://localhost:{port}/{dataset}/sparql");

    docker_rm(container);
    run(["docker", "run", "-d", "--name", container, "-p", f"{port}:3030", FUSEKI_IMAGE, "--mem", f"/{dataset}"]);
    wait_for_port("localhost", port);
    print(f"Fuseki ready at http://localhost:{port}/{dataset}/sparql (requested: {endpoint})");

# preprocess calls

def run_preprocess_redis(redis_db: int):
    cmd = ["python", PREPROCESS_DIR, INPUT_DIR + "/meta", TEMP_DIR + "/meta-preprocessed", "--storage-type", "redis", "--redis-db", redis_db];
    run(cmd);
    print("Input for meta preprocessed");
    cmd = ["python", PREPROCESS_DIR, INPUT_DIR + "/index", TEMP_DIR + "/index-preprocessed", "--storage-type", "redis", "--redis-db", redis_db];
    run(cmd);
    print("Input for index preprocessed.");

def run_preprocess_sparql(sparql_endpoint: str):
    cmd = ["python", PREPROCESS_DIR, INPUT_DIR + "/meta", TEMP_DIR + "/meta-preprocessed", "--storage-type", "sparql", "--sparql-endpoint", sparql_endpoint];
    run(cmd);
    print("Input for meta preprocessed");
    cmd = ["python", PREPROCESS_DIR, INPUT_DIR + "/index", TEMP_DIR + "/index-preprocessed", "--storage-type", "sparql", "--sparql-endpoint", sparql_endpoint];
    run(cmd);
    print("Input for index preprocessed.");

# preprocess pipelines

def preprocess_pipeline_with_redis(redis_db: int):
    try:
        start_redis();
        run_preprocess_redis(redis_db);
    finally:
        docker_rm(REDIS_CONTAINER);

def preprocess_pipeline_with_sparql(sparql_endpoint: str):
    try:
        start_fuseki_for_endpoint(sparql_endpoint);
        run_preprocess_sparql(sparql_endpoint);
    finally:
        docker_rm(FUSEKI_CONTAINER);

# luigi tasks

class Preprocess(luigi.Task):
    param = luigi.Parameter(default = 42);

    def run(self):

        print("Running task Preprocess");
        print("Placeholder - call preprocess with all files in INPUT_DIR and store output in TEMP_DIR");

        if(PREPROCESS_STORAGE_TYPE == "sparql"):
            preprocess_pipeline_with_sparql(PREPROCESS_SPARQL_ENDPOINT);
        elif(PREPROCESS_STORAGE_TYPE == "redis"):
            preprocess_pipeline_with_redis(PREPROCESS_REDIS_DB_NUMBER);
        else:
            print("Incorrect value for PREPROCESS_STORAGE_TYPE");

        print("Finished task Preprocess");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)
    
class Validation(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Preprocess(self.param);

    def run(self):
        print("Running task Validation");

        # oc_validator for meta CSVs
        folder = Path("dir/temp/meta-preprocessed");
        counter = 0;
        for file in folder.iterdir():
            if file.is_file():
                if VALIDATION_TYPE == "0":
                    v = Validator(str(file), TEMP_DIR + "/meta-validated");
                    v.validate();
                if VALIDATION_TYPE == "1":
                    v = Validator(str(file), TEMP_DIR + "/meta-validated", use_meta_endpoint = True);
                    v.validate();
                if VALIDATION_TYPE == "2":
                    v = Validator(str(file), TEMP_DIR + "/meta-validated", verify_id_existence = False);
                    v.validate();
                counter += 1;
                print("Validated META file no. " + str(counter));

        # oc_validator for index CSVs
        # oc_validator currently rejects preprocessed index CSVs
        folder = Path("dir/temp/index-preprocessed");
        counter = 0;
        for file in folder.iterdir():
            if file.is_file():
                if VALIDATION_TYPE == "0":
                    v = Validator(str(file), TEMP_DIR + "/index-validated");
                    v.validate();
                if VALIDATION_TYPE == "1":
                    v = Validator(str(file), TEMP_DIR + "/index-validated", use_meta_endpoint = True);
                    v.validate();
                if VALIDATION_TYPE == "2":
                    v = Validator(str(file), TEMP_DIR + "/index-validated", verify_id_existence = False);
                    v.validate();
                counter += 1;
                print("Validated META file no. " + str(counter));
        #TODO?: eliminate incorrectly validated lines?

        print("Finished task Validation");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

class DatabaseSwitchOn(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Preprocess(self.param);

    def run(self):
        print("Running task DatabaseSwitchOn");
        
        #TODO turn on META (Blazegraph?), PROV (Virtuoso apparently?) and INDEX (QLEVER in Docker) dbs ig
        print("Placeholder - turn on META, PROV and INDEX");
        
        # triplestore for META (ask Arcangelo which one)


        # QLEVER in Docker for INDEX???


        # Virtuoso in Docker for PROV
        cmd = ["python", OC_VIRTUOSO_UTILITIES_DIR + "/launch_virtuoso.py"];
        if PROV_VIRTUOSO_CUSTOM == 1:
            if PROV_VIRTUOSO_NAME != "":
                cmd.append("--name");
                cmd.append(PROV_VIRTUOSO_NAME);
            if PROV_VIRTUOSO_HTTP_PORT != "":
                cmd.append("--http-port");
                cmd.append(PROV_VIRTUOSO_HTTP_PORT);
            if PROV_VIRTUOSO_ISQL_PORT != "":
                cmd.append("--isql-port");
                cmd.append(PROV_VIRTUOSO_ISQL_PORT);
            if PROV_VIRTUOSO_DATA_DIR != "":
                cmd.append("--data-dir");
                cmd.append(PROV_VIRTUOSO_DATA_DIR);
            if PROV_VIRTUOSO_DBA_PASSWORD != "":
                cmd.append("--dba-password");
                cmd.append(PROV_VIRTUOSO_DBA_PASSWORD);
            if PROV_VIRTUOSO_MOUNT_VOLUME != "":
                cmd.append("--mount-volume");
                cmd.append(PROV_VIRTUOSO_MOUNT_VOLUME);
            if PROV_VIRTUOSO_NETWORK != "":
                cmd.append("--network");
                cmd.append(PROV_VIRTUOSO_NETWORK);
            if PROV_VIRTUOSO_MEMORY != "":
                cmd.append("--memory");
                cmd.append(PROV_VIRTUOSO_MEMORY);
            if PROV_VIRTUOSO_DETACH == 1:
                cmd.append("--detach");
            if PROV_VIRTUOSO_WAIT_READY == 1:
                cmd.append("--wait-ready");
            if PROV_VIRTUOSO_ENABLE_WRITE_PERMISSIONS == 1:
                cmd.append("--enable-write-permissions");
        cmd.append("--mount-volume");
        cmd.append(PROV_VIRTUOSO_DUMP_DIR);
        run(cmd);

        # virtuoso_utilities/bulk_load.py n-quads to populate PROV if enabled
        if PROV_VIRTUOSO_BULK_LOAD:
            cmd = ["python", OC_VIRTUOSO_UTILITIES_DIR + "/bulk_load.py"];
            cmd.append("--data-directory");
            cmd.append(PROV_VIRTUOSO_BULK_LOAD_DIR);
            cmd.append("--password");
            if PROV_VIRTUOSO_DBA_PASSWORD != "":
                cmd.append(PROV_VIRTUOSO_DBA_PASSWORD);
            else:
                cmd.append("dba");
            if PROV_VIRTUOSO_CUSTOM == 1:
                if PROV_VIRTUOSO_NAME != "":
                    cmd.append("--docker-container");
                    cmd.append(PROV_VIRTUOSO_NAME);
                if PROV_VIRTUOSO_ISQL_PORT != "":
                    cmd.append("--port");
                    cmd.append(PROV_VIRTUOSO_ISQL_PORT);
                if PROV_VIRTUOSO_DBA_USERNAME != "":
                    cmd.append("--user");
                    cmd.append(PROV_VIRTUOSO_DBA_USERNAME);
            cmd.append("--recursive");
            run(cmd);
        
        print("Finished task DatabaseSwitchOn");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

class OCMeta(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Validator(self.param), DatabaseSwitchOn(self.param);

    def run(self):
        print("Running task OCMeta");
        
        #TODO create config in yaml based on the meta configuration values
        config = {}
        config["triplestore_url"] = META_TRIPLESTORE_URL;
        config["provenance_triplestore_url"] = META_PROVENANCE_TRIPLESTORE_URL;
        config["provenance_endpoints"] = "[]";
        config["input_csv_dir"] = TEMP_DIR + "/meta-preprocessed";
        config["base_output_dir"] = META_OUTPUT_DIR;

        config["output_rdf_dir"] = META_OUTPUT_RDF_DIR;
        config["base_iri"] = META_BASE_IRI;
        config["context_path"] = META_CONTEXT_PATH;
        config["dir_split_number"] = META_DIR_SPLIT_NUMBER;
        config["items_per_file"] = META_ITEMS_PER_FILE;
        config["default_dir"] = META_DEFAULT_DIR;
        config["supplier_prefix"] = "'" + META_SUPPLIER_PREFIX + "'";
        if META_RDF_OUTPUT_IN_CHUNKS:
            config["rdf_output_in_chunks"] = "True";
        else:
            config["rdf_output_in_chunks"] = "False";
        if META_ZIP_OUTPUT_RDF:
            config["zip_output_rdf"] = "True";
        else:  
            config["zip_output_rdf"] = "False";
        config["source"] = META_SOURCE;
        if META_USE_DOI_API_SERVICE:
            config["use_doi_api_service"] = "True";
        else:
            config["use_doi_api_service"] = "False";
        config["workers_number"] = META_WORKERS_NUMBER;
        config["silencer"] = META_SILENCER;
        if META_GENERATE_RDF_FILES:
            config["generate_rdf_files"] = "True";
        else:
            config["generate_rdf_files"] = "False";
        config["virtuoso_full_text_search"] = "True";
        
        


        path = Path("dir/temp/meta_config.yaml")
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(config, f, sort_keys=False, default_flow_style=False)

        #TODO call oc_meta to update META and PROV with validated meta data
        print("Placeholder - call oc_meta to update META and PROV with validated meta data");
        

        print("Finished task OCMeta");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)
    
class OCMetaVal(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return OCMeta(self.param);

    def run(self):
        print("Running task OCMetaVal");
        
        #TODO validate new data in META nad PROV with oc_meta_val
        print("Placeholder - call oc_meta_val to validate new data in META and PROV");
        
        print("Finished task OCMetaVal");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

class OCMetaCsv(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return OCMetaVal(self.param);

    def run(self):
        print("Running task OCMetaCsv");
        
        #TODO if good then call oc_meta_csv to construct meta.csv with data from META
        print("placeholder - call oc_meta_csv to construct meta.csv with data from META");
        
        print("Finished task OCMetaCsv");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)
    
class Meta2Redis(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return OCMetaCsv(self.param);

    def run(self):
        print("Running task Meta2Redis");
        
        #TODO turn on in-RAM REDIS?
        print("Placeholder - turn on in-RAM REDIS");

        #TODO call meta2redis to upload the data from constructed meta.csv to in-RAM REDIS
        print("Placeholder - call meta2redis to upload the data from constructed meta.csv to in-RAM REDIS");
        
        print("Finished task Meta2Redis");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

class OCIndex(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Meta2Redis(self.param);

    def run(self):
        print("Running task OCIndex");
        
        #TODO call oc_index to read data from citations input file and in-RAM REDIS to update PROV and create raw data
        print("Placeholder - call oc_index to read data from citations input file and in-RAM REDIS to update PROV and create raw data");
        
        print("Finished task OCIndex");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

class Upload(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return OCIndex(self.param);

    def run(self):
        print("Running task Upload");
        
        #TODO call upload to use raw data to update INDEX and PROV(?)
        print("Placeholder - call upload to use raw data to update INDEX and PROV?");
        
        #TODO: virtuoso_utilities/dump_quadstore.py to get PROV dump
        if PROV_VIRTUOSO_DUMP:
            cmd = ["python", OC_VIRTUOSO_UTILITIES_DIR + "/dump_quadstore.py"];
            cmd.append("--password")
            if PROV_VIRTUOSO_DBA_PASSWORD != "":
                cmd.append(PROV_VIRTUOSO_DBA_PASSWORD);
            else:
                cmd.append("dba");
            if PROV_VIRTUOSO_CUSTOM == 1:
                cmd.append("--port");
                cmd.append(PROV_VIRTUOSO_ISQL_PORT);
                cmd.append("--user");
                cmd.append(PROV_VIRTUOSO_DBA_USERNAME);
            cmd.append("--docker-container");
            cmd.append(PROV_VIRTUOSO_NAME);
            cmd.append("--file-length-limit");
            cmd.append(PROV_VIRTUOSO_DUMP_FILE_LIMIT);
            if not PROV_VIRTUOSO_DUMP_COMPRESSION:
                cmd.append("--no-compression");
            run(cmd);

        print("Finished task Upload");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)
    
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
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

if __name__ == "__main__":

    task_preprocess = Preprocess();
    #task_validation = Validation();
    task_dbswitchon = DatabaseSwitchOn();
    task_ocmeta = OCMeta();
    task_ocmetaval = OCMetaVal();
    task_ocmetacsv = OCMetaCsv();
    task_meta2redis = Meta2Redis();
    task_ocindex = OCIndex();
    task_upload = Upload();
    task_publication = Publication();

    start = time.time();
    print("");
    task_preprocess.run();
    #task_validation.run();
    task_dbswitchon.run();
    task_ocmeta.run();
    task_ocmetaval.run();
    task_ocmetacsv.run();
    task_meta2redis.run();
    task_ocindex.run();
    task_upload.run();
    task_publication.run();
    end = time.time();
    print("Total runtime: " + str(end-start) + "s");