#!/bin/bash
bq query --use_legacy_sql=false 'CALL `letest-409019.LETestDataset.createResultTables`()'
bq query --use_legacy_sql=false 'CALL `letest-409019.LETestDataset.createResultTables`()'

bq load --source_format=CSV --skip_leading_rows=1 LETestDataset.wialon gs://letest_bucket_1/wialon_dump.csv
bq load --source_format=CSV --skip_leading_rows=1 LETestDataset.telematics gs://letest_bucket_1/telematics_dump.csv
bq load --source_format=NEWLINE_DELIMITED_JSON LETestDataset.fendt_data gs://letest_bucket_1/fendt_data_partitioned.json
bq load --source_format=NEWLINE_DELIMITED_JSON LETestDataset.fendt_gps gs://letest_bucket_1/fendt_gps.json

bq query --use_legacy_sql=false 'CALL `letest-409019.LETestDataset.createFendt`()'
bq query --use_legacy_sql=false 'CALL `letest-409019.LETestDataset.createMachineIdLink`()'
bq query --use_legacy_sql=false 'CALL `letest-409019.LETestDataset.populateJoinedData`()'
bq query --use_legacy_sql=false 'CALL `letest-409019.LETestDataset.createMultilines`()'
bq query --use_legacy_sql=false 'CALL `letest-409019.LETestDataset.saveResults`()'
