# -*- coding: utf-8 -*-
# Generate configuration for the test runs. In each run, one parameters ranges in all values while the remaining parameters are given the default value.
import codecs
import os
import sys
from string import Template

filename = "../../../run_SPARE.sh"

datasets = [
    Template("join__${table}__${minsize}__${minsup}__${bins}__${timescale}__${bint}"),
    Template("tmp_transactiontable__tbl_${table}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bins}__ts_${timescale}__bint_${bint}")
]
configs = [
    {"table": "oldenburg_standard_10000", "minsize": "1000", "minsup": "20", "bins": "20", "timescale": "absolute", "bint": "5"},
    {"table": "oldenburg_standard_50000", "minsize": "1000", "minsup": "20", "bins": "20", "timescale": "absolute", "bint": "5"},
    {"table": "oldenburg_standard_100000", "minsize": "1000", "minsup": "20", "bins": "20", "timescale": "absolute", "bint": "5"},
    {"table": "oldenburg_standard_250000", "minsize": "1000", "minsup": "20", "bins": "20", "timescale": "absolute", "bint": "5"},
    {"table": "oldenburg_standard_500000", "minsize": "1000", "minsup": "20", "bins": "20", "timescale": "absolute", "bint": "5"},
    {"table": "oldenburg_standard", "minsize": "1000", "minsup": "20", "bins": "20", "timescale": "absolute", "bint": "5"},
]


def giveExecutionPermissionToFile(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | 0o111)


runs = []
with codecs.open(filename, "w", "utf-8") as w:
    runs.append("#!/bin/bash")
    runs.append("set -e")
    for dataset in datasets:
        for config in configs:
            command = """spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console" --conf "spark.executor.extraJavaOptions=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8090 -Dcom.sun.management.jmxremote.rmi.port=8091 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false" --conf spark.memory.fraction=0.8 --conf spark.driver.maxResultSize=10g --driver-memory 8G --class it.unibo.tip.main.Main build/libs/SPARE-all.jar --input=/user/mfrancia/spare/input/${dataset}  --output=/user/mfrancia/spare/input/output/ --m=1000 --k=20 --l=1 --g=10000 --executors=10 --cores=2 --ram=8g"""
            config["dataset"] = dataset.substitute(config)
            command = Template(command).substitute(config)
            # for key, value in config.items():
            #     try:
            #         dataset = dataset.format(key=value)
            #     except:

            #         pass
            #     try:
            #         command = command.format(key=value)
            #     except:
            #         pass
            # command = command.format(dataset=dataset)
            runs.append(command)
    giveExecutionPermissionToFile(filename)
    print("Done. Nruns: " + str(len(runs)))
    w.write('\n'.join(runs))
