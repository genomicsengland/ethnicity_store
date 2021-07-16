"""
read ethnicity data from the APC database
"""

from modules import database
from classes import diversity_db

sql = """
select distinct participant_id as id
    ,ethnos as ethnicity_code
    ,admidate::date as source_date
from q4_19_nhsd.apc
where ethnos not in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') and
ethnos is not null and
admidate is not null
;
"""

def run_etl(c, s):

    e = database.get_engine(c.hes_db_conn_str)

    cr = database.run_sql_query(e, sql)

    d = cr.mappings().all()

    # gather all participants
    pids = set([x['id'] for x in d])

    # make the observed partiicpants
    obsd = {x: diversity_db.ObservedParticipant(s, x, '100k_ca', True, '100k') for x in pids}

    # add the ethnicities
    for x in d:

        obsd[x['id']].reported_ethnicities.append(
            diversity_db.ObservedReportedEthnicity(s, x['id'], x['ethnicity_code'], 'hes_apc', x['source_date'])
        )

    # add to session
    for pid, o in obsd.items():

        o.add_to_db(s)
