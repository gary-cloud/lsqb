#!/usr/bin/env python3
import kuzu

DB_PATH = '/home/gary/ldbc_snb/lsqb/kuzu/scratch/lsqb-database'
QUERY = """
MATCH (p:Person)-[:Person_knows_Person]->(friend:Person)
RETURN COUNT(DISTINCT p) AS num_people, COUNT(*) AS num_knows;
"""

conn = kuzu.Connection(kuzu.Database(DB_PATH))
result = conn.execute(QUERY)
print(result.get_all())
