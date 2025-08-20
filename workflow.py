import luigi;

INPUT_DIR = "";
TEMP_DIR = "";
OUTPUT_DIR = "";
PREPROCESS_DIR = "oc_meta/oc_meta/run/meta/preprocess_input.py";
OC_VALIDATOR_DIR = "oc_validator/oc_validator/main.py";
OC_META_DIR = "oc_meta/oc_meta/run/meta_process.py";
OC_META_VAL_DIR = "oc_meta/oc_meta/run/meta/check_results.py";
OC_META_CSV = "oc_meta/oc_meta/run/csv_generator_lite.py";
META2REDIS_DIR = "index/scripts/ocworkflow.py/populate_redis()";
OC_INDEX_DIR = "index/scripts/ocworkflow.py/gen_zipbatch()";
UPLOAD_DIR = "";
PUBLICATION_DIR = "";

#preprocess_input default values
PREPROCESS_REDIS_HOST = "localhost";
PREPROCESS_REDIS_PORT = 6379;
PREPROCESS_REDIS_DB_NUMBER = 10;
PREPROCESS_SPARQL_ENDPOINT = "";

class Preprocess(luigi.Task):
    param = luigi.Parameter(default = 42);

    def run(self):

        print("Running task Preprocess");
        #TODO input files
        print("Step 1 - placeholder");

        #TODO call existing data from ORACLE (REDIS)?
        print("Step 2 - placeholder");

        #TODO calling preprocess on input files and hold two output files
        print("Step 3 - placeholder");

        print("Finished task Preprocess");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)
    
class Validator(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Preprocess(self.param);

    def run(self):
        print("Running task Validator");
        
        #TODO put them in oc_validator and return validated versions
        print("Step 4 - placeholder");
        
        print("Finished task Validator");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

class DatabaseSwitchOn(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Validator(self.param);

    def run(self):
        print("Running task DatabaseSwitchOn");
        
        #TODO turn on META (Virtuoso?), PROV (Virtuoso?) and INDEX (QLEVER in Docker) dbs ig
        print("Step 5 - placeholder");
        
        print("Finished task DatabaseSwitchOn");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

class OCMeta(luigi.Task):
    param = luigi.Parameter(default = 42);

    def requires(self):
        return Validator(self.param), DatabaseSwitchOn(self.param);

    def run(self):
        print("Running task OCMeta");
        
        #TODO call oc_meta to update META and PROV with validated meta data
        print("Step 6 - placeholder");
        
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
        print("Step 7 - placeholder");
        
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
        print("Step 8 - placeholder");
        
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
        print("Step 9 - placeholder");

        #TODO call meta2redis to upload the data from constructed meta.csv to in-RAM REDIS
        print("Step 10 - placeholder");
        
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
        print("Step 11 - placeholder");
        
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
        print("Step 12 - placeholder");
        
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
        print("Step 13 - placeholder");
        
        print("Finished task Publication");

    def output(self):
        return luigi.LocalTarget("dupa-%s.txt" % self.param)

if __name__ == "__main__":

    task_preprocess = Preprocess();
    task_validator = Validator();
    task_dbswitchon = DatabaseSwitchOn();
    task_ocmeta = OCMeta();
    task_ocmetaval = OCMetaVal();
    task_ocmetacsv = OCMetaCsv();
    task_meta2redis = Meta2Redis();
    task_ocindex = OCIndex();
    task_upload = Upload();
    task_publication = Publication();

    task_preprocess.run();
    task_validator.run();
    task_dbswitchon.run();
    task_ocmeta.run();
    task_ocmetaval.run();
    task_ocmetacsv.run();
    task_meta2redis.run();
    task_ocindex.run();
    task_upload.run();
    task_publication.run();