"""
Utilities for xena queries.

A basic query example.
Queries are scheme expressions.

>>> import xena_query as xena
>>> xena.post("https://genome-cancer.ucsc.edu/proj/public/xena", "(+ 1 2)")
'3.0'

>>> xena.post("https://genome-cancer.ucsc.edu/proj/public/xena", "(let [x 2 y (+ x 3)] (* x y))")
'10.0'

Looking up sample ids for the TCGA LGG cohort.

>>> xena.post("https://genome-cancer.ucsc.edu/proj/public/xena",
               xena.patient_to_sample_query("TCGA.LGG.sampleMap",
                                            ["TCGA-CS-4938",
                                             "TCGA-HT-7693",
                                             "TCGA-CS-6665",
                                             "TCGA-S9-A7J2",
                                             "TCGA-FG-A6J3"]))
'{"TCGA.LGG.sampleMap":["TCGA-CS-4938-01","TCGA-CS-6665-01","TCGA-FG-A6J3-01","TCGA-HT-7693-01","TCGA-S9-A7J2-01"]}'
>>> import json
>>> json.loads(_)
{u'TCGA.LGG.sampleMap': [u'TCGA-CS-4938-01', u'TCGA-CS-6665-01', u'TCGA-FG-A6J3-01', u'TCGA-HT-7693-01', u'TCGA-S9-A7J2-01']}
"""
import urllib2

def quote(s):
    return '"' + s + '"'

def array_fmt(l):
    return '[' + ', '.join((quote(s) for s in l)) + ']'

sample_query_str = """
(let [cohort %s
      patient-dataset (car (query {:select [[:field.id :patient] [:dataset.id :dataset]]
                                   :from [:dataset]
                                   :left-join [:field [:= :dataset_id :dataset.id]]
                                   :where [:and [:= :cohort cohort]
                                                [:= :field.name "_PATIENT"]]}))
      patient (:PATIENT patient-dataset)
      dataset (:DATASET patient-dataset)
      sample (:ID (car (query {:select [:field.id]
                               :from [:field]
                               :where [:and [:= :dataset_id dataset]
                                            [:= :field.name "sampleID"]]})))
      N (- (/ (:N (car (query {:select [[#sql/call [:sum #sql/call [:length :scores]] :N]]
                  :from [:field_score]
                  :join [:scores [:= :scores_id :scores.id]]
                  :where [:= :field_id patient]}))) 4) 1)]
  {cohort (map :SAMPLE (query {:select [:sample]
                               :from [{:select [[#sql/call [:unpackValue patient, :x] :patient]
                                                [#sql/call [:unpackValue sample, :x]  :sample]]
                                       :from [#sql/call [:system_range 0 N]]}]
                               :where [:in :patient %s]}))})
"""

def patient_to_sample_query(cohort, patients):
    """Return a xena query which looks up sample ids for the given patients."""
    return sample_query_str % (quote(cohort), array_fmt(patients))

headers = { 'Content-Type' : "text/plain" }

def post(url, query):
    """POST a xena data query to the given url."""
    req = urllib2.Request(url + '/data/', query, headers)
    response = urllib2.urlopen(req)
    result = response.read()
    return result
