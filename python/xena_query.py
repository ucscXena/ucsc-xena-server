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

>>> r = xena.post("https://genome-cancer.ucsc.edu/proj/public/xena",
                  xena.patient_to_sample_query("TCGA.LGG.sampleMap",
                                               ["TCGA-CS-4938",
                                                "TCGA-HT-7693",
                                                "TCGA-CS-6665",
                                                "TCGA-S9-A7J2",
                                                "TCGA-FG-A6J3"]))
'{"TCGA.LGG.sampleMap":["TCGA-CS-4938-01","TCGA-CS-6665-01","TCGA-FG-A6J3-01","TCGA-HT-7693-01","TCGA-S9-A7J2-01"]}'

>>> r = xena.post("https://genome-cancer.ucsc.edu/proj/public/xena",
                  xena.find_sample_by_field_query("TCGA.LGG.sampleMap",
                                                    "_PATIENT",
                                                    ["TCGA-CS-4938",
                                                     "TCGA-HT-7693",
                                                     "TCGA-CS-6665",
                                                     "TCGA-S9-A7J2",
                                                     "TCGA-FG-A6J3"]))
'{"TCGA.LGG.sampleMap":["TCGA-CS-4938-01","TCGA-CS-6665-01","TCGA-FG-A6J3-01","TCGA-HT-7693-01","TCGA-S9-A7J2-01"]}'
>>> import json
>>> json.loads(r)
{u'TCGA.LGG.sampleMap': [u'TCGA-CS-4938-01', u'TCGA-CS-6665-01', u'TCGA-FG-A6J3-01', u'TCGA-HT-7693-01', u'TCGA-S9-A7J2-01']}
"""

import urllib2

def compose1(f, g):
    def composed(*args, **kwargs):
        return f(g(*args, **kwargs))
    return composed

# funcitonal composition, e.g.
# compose(f, g)(a, ...) == f(g(a, ...))
compose = lambda *funcs: reduce(compose1, funcs)

def quote(s):
    return '"' + s + '"'

def array_fmt(l):
    return '[' + ', '.join((quote(s) for s in l)) + ']'

# The strategy here is
#   o Do table scan on code to find codes matching field values
#   o Do IN query on unpack(field, x) to find rows matching codes
#   o Project to unpack(sample, x) to get sampleID code
#   o Join with code to get sampleID values
#
# Note the :limit on the table scan. This makes the table scan exit after we've
# found enough values, rather than continuing to the end. We can do this because
# enumerated values are unique. An alternative would be to index all the enumerated
# values in the db.
sample_query_str = """
(let [cohort %s
      field_id-dataset (car (query {:select [[:field.id :field_id] [:dataset.id :dataset]]
                                    :from [:dataset]
                                    :join [:field [:= :dataset_id :dataset.id]]
                                    :where [:and [:= :cohort cohort]
                                                 [:= :field.name %s]]}))
      values %s
      field_id (:field_id field_id-dataset)
      dataset (:dataset field_id-dataset)
      sample (:id (car (query {:select [:field.id]
                               :from [:field]
                               :where [:and [:= :dataset_id dataset]
                                            [:= :field.name "sampleID"]]})))
      N (- (:rows (car (query {:select [:rows]
                               :from [:dataset]
                               :where [:= :id dataset]}))) 1)]
  {cohort (map :value (query {:select [:value]
                              :from [{:select [:x #sql/call [:unpack field_id, :x]]
                                      :from [#sql/call [:system_range 0 N]]
                                      :where [:in #sql/call [:unpack field_id, :x] {:select [:ordering]
                                                                                             :from [:code]
                                                                                             :where [:and [:= :field_id field_id]
                                                                                                          [:in :value values]]
                                                                                             :limit (count values)}]}]
                              :join [:code [:and [:= :field_id sample]
                                                 [:= :ordering #sql/call [:unpack sample :x]]]]}))})
"""

cohort_query_str = """
(map :cohort (query {:select [:%distinct.cohort]
                     :from [:dataset]
                     :where [:not [:is nil :cohort]]}))
"""

datasets_list_in_cohort_query = """
(map :text (query {:select [:text]
                   :from [:dataset]
                   :where [:= :cohort %s ]})
"""

def find_sample_by_field_query(cohort, field, values):
    """Return a xena query which looks up sample ids for the given field=values."""
    return sample_query_str % (quote(cohort), quote(field), array_fmt(values))

def patient_to_sample_query(cohort, patients):
    """Return a xena query which looks up sample ids for the given patients."""
    return find_sample_by_field_query(cohort, "_PATIENT", patients)

headers = { 'Content-Type' : "text/plain" }

def post(url, query):
    """POST a xena data query to the given url."""
    req = urllib2.Request(url + '/data/', query, headers)
    response = urllib2.urlopen(req)
    result = response.read()
    return result

def find_cohorts():
    """ Return a list of cohorts on a host at a specific url """
    """ return example: ["chinSF2007_public","TCGA.BRCA.sampleMap","cohort3"] """
    return cohort_query_str

def find_datasets_in_cohort(url, cohort):
    """ Returen a list of datasets in a specific cohort on server=url """
    """ Each dataset is a dictionary of the data's metadata"""
    return map(json.loads,
            json.loads(post(url, datasets_list_in_cohort_query % (quote(cohort)))))
