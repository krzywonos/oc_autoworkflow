def main():
    #TODO input files
    print("Step 1 - placeholder");

    #TODO call existing data from ORACLE (REDIS)?
    print("Step 2 - placeholder");

    #TODO calling preprocess on input files and hold two output files
    print("Step 3 - placeholder");

    #TODO put them in oc_validator and return validated versions
    print("Step 4 - placeholder");

    #TODO turn on META (Virtuoso?), PROV (Virtuoso?) and INDEX (QLEVER in Docker) dbs ig
    print("Step 5 - placeholder");

    #TODO call oc_meta to update META and PROV with validated meta data
    print("Step 6 - placeholder");

    #TODO validate new data in META nad PROV with oc_meta_val
    print("Step 7 - placeholder");

    #TODO if good then call oc_meta_csv to construct meta.csv with data from META
    print("Step 8 - placeholder");

    #TODO turn on in-RAM REDIS?
    print("Step 9 - placeholder");

    #TODO call meta2redis to upload the data from meta.csv to in-RAM REDIS
    print("Step 10 - placeholder");

    #TODO call oc_index to read data from citations input file and in-RAM REDIS to update PROV and create raw data
    print("Step 11 - placeholder");

    #TODO call upload to use raw data to update INDEX and PROV(?)
    print("Step 12 - placeholder");

    #TODO? ?maybe? ?call? ?publication? ?with? ?raw? ?data?
    print("Step 13 - placeholder");


if __name__ == "__main__":
    main();