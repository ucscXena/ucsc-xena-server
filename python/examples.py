#!/usr/bin/env python

import xena_query as xena

huburl = "https://genome-cancer.ucsc.edu/proj/public/xena"
dataset = "public/TCGA/TCGA.PANCAN.sampleMap/HiSeqV2"
samples = ["TCGA-44-6778-01","TCGA-VM-A8C8-01"]
identifiers = ["TP53"]


# Find all the identifiers of a dataset
allidentifiers = xena.dataset_field(huburl, dataset)
print allidentifiers[-10:]
#'[u'ZXDB', u'ZXDC', u'ZYG11A', u'ZYG11B', u'ZYX', u'ZZEF1', u'ZZZ3', u'psiTPTE22', u'sampleID', u'tAKR']'

#Find all the samples of a dataset
allsamples = xena.dataset_samples(huburl, dataset)
print allsamples[:5]
#'[u'TCGA-S9-A7J2-01', u'TCGA-G3-A3CH-11', u'TCGA-EK-A2RE-01', u'TCGA-44-6778-01', u'TCGA-VM-A8C8-01']'

#Find value matrix of a particular set of samples and identifiers (genes, probes, isoforms)
values = xena.dataset_probe_values(huburl, dataset, samples, identifiers)
print values
#[[10.4169, 9.6591]]

