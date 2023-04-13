from com.db.fw.metadata.setup import MetadataSetup

if __name__ == "__main__":
    setup = MetadataSetup(db_name = "default1", debug = True, dropTables = True)
    setup.run()