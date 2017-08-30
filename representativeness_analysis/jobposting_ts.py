from collections import defaultdict

import json
import logging

from skills_ml.datasets import job_postings
from airflow.hooks import S3Hook
s3_conn = S3Hook().get_conn()


ts = defaultdict(int)


quarters = []
for y in range(1, 5):
    for q in range(1, 5):
        quarters.append('201'+str(y)+'Q'+str(q))

quarters.remove('2011Q1')

for q in quarters:
	job_postings_generator = job_postings(s3_conn, q, 'open-skills-private/job_postings_common')
	logging.info('Counting jobposting for {}'.format(q))
	for jp in job_postings_generator:
		d = json.loads(jp)
		if d['id'][:3] != 'NLX':
			date = d['datePosted']
			ts[date] = ts[date] + 1


with open('jobposting_2011_2014_v1.json', 'w') as fp:
	json.dump(ts, fp)
