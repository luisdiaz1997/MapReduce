#!/usr/bin/env python

import pyspark
import tarfile
from io import BytesIO
import sys


def extractFiles(incoming_bytes):
    tar = tarfile.open(fileobj=BytesIO(incoming_bytes), mode="r:gz")
    return [tar.extractfile(x).read() for x in tar if x.isfile()]


if len(sys.argv) != 3:
    raise Exception("Exactly 2 arguments are required: <outputUri>")

sc = pyspark.SparkContext(appName="ana")

gzfiles = sc.binaryFiles(sys.argv[1])
values = gzfiles.flatMap(lambda x : extractFiles(x[1])).filter(lambda x: len(x)>1).map(lambda x: x.decode("latin-1"))
words = values.flatMap(lambda line: line.split(" ")).filter(lambda x: (len(x) > 1) & x.isalpha())
anagrams = words.map(lambda word: (''.join(sorted(word.lower())), set([word.lower()]))).reduceByKey(lambda a,b: a.union(b) ).filter(lambda x: len(x[1]) > 1).map(lambda x: x[1])

anagrams.saveAsTextFile(sys.argv[2])

sc.stop()
