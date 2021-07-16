"""
test the basic operation of diversity_db
"""

import logging
import unittest
import time
import sys
from sqlalchemy import and_
from config import ConfigFactory
from modules import database, concept, log
from classes import diversity_db

c = ConfigFactory.factory()
log.setup_logger(c)

LOGGER = logging.getLogger(__name__)

def reset_observed_classes():
    # reset all the observed classes, otherwise invalid concept cids carried
    # across between tests
    diversity_db.ObservedParticipant.group_concept_codes = None
    diversity_db.ObservedParticipant.programme_concept_codes = None
    diversity_db.ObservedParticipant.participants_in_db = None
    diversity_db.ObservedReportedEthnicity.ethnicity_concept_codes = None
    diversity_db.ObservedReportedEthnicity.source_concept_codes = None

def get_concept_cid(s, concept_code, codesystem):

    d = s.query(diversity_db.Concept.uid).filter(
        and_(diversity_db.Concept.concept_code == concept_code,
                diversity_db.Concept.codesystem == codesystem)
    ).all()
    assert len(d) == 1, 'too many cids returned'

    return d[0][0]

def get_best_ethnicity_for_participant(pid):

    sql = f"select v.best_ethnicity_code from ethnicity_store.vw_participant_ethnicity v where v.participant_id = '{pid}';"
    e = database.get_engine(c.div_db_conn_str)
    cr = database.run_sql_query(e, sql)
    d = cr.mappings().all()

    assert len(d) == 1, 'too many best ethnicities returned'

    return d[0]['best_ethnicity_code']


class DiversityDBOperation(unittest.TestCase):

    def setUp(self):

        self.c = ConfigFactory.factory()

        database.create_diversity_db(c)

        self.s = database.make_session(c)

    def tearDown(self):

        self.s.close()

        database.drop_diversity_db(c)

        reset_observed_classes()
        
    def test_insert(self):
        """
        test can write and retrieve from db
        """

        a = diversity_db.Concept(
            concept_code='test',
            codesystem='test'
        )
        self.s.add(a)
        self.s.commit()
        d = self.s.query(diversity_db.Concept).all()
        self.assertTrue(len(d) == 1)

    def test_concept_upload(self):
        """
        test we can upload all the concept data
        """

        concept.populate_concept_table(self.s)

        written_length = len(self.s.query(diversity_db.Concept).all())

        with open(concept.CONCEPT_DATA_FP, "r") as f:

            expected_length = len(f.readlines()) - 1

        self.assertEqual(written_length, expected_length)

    def test_participant_merge(self):
        """
        test participant data is updated as and when it changes
        """

        concept.populate_concept_table(self.s)

        # write participant
        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k'}
        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        # toggle in_ngrl and recreate participant
        d['in_ngrl'] = False
        b = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        # pull participant from db and check only one participant and that
        # in_ngrl has been switched
        p = self.s.query(diversity_db.Participant).all()
        self.assertTrue(len(p) == 1)
        self.assertNotEqual(p[0].in_ngrl, d['in_ngrl'])

    def test_participant_ethnicity_creation(self):
        """
        check we can write a participant with ethnicity data
        """

        concept.populate_concept_table(self.s)

        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k',
             'reported_ethnicities': [
                 {'ethnicity_code': 'A',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
             ]}

        # create participant and then write data
        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        # check that each of the attributes have been converted correctly
        p = self.s.query(diversity_db.Participant).first()
        group_cid = get_concept_cid(self.s, d['group'], 'group')
        programme_cid = get_concept_cid(self.s, d['programme'], 'programme')

        self.assertEqual(p.group_cid, group_cid)
        self.assertEqual(p.programme_cid, programme_cid)
        self.assertEqual(p.id, d['id'])
        self.assertEqual(p.in_ngrl, d['in_ngrl'])

    def test_best_ethnicity_view_a(self):

        concept.populate_concept_table(self.s)

        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k',
             'reported_ethnicities': [
                 {'ethnicity_code': 'A',
                  'source': 'dams',
                  'source_date': '2000-01-01'},
                 {'ethnicity_code': 'B',
                  'source': 'hes_apc',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'C',
                  'source': 'hes_apc',
                  'source_date': '1900-01-01'},
             ]}

        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        be = get_best_ethnicity_for_participant(d['id'])

        # this case should be A as all ethnicities equally common but A is
        # more recent
        self.assertEqual(be, 'A')

    def test_best_ethnicity_view_b(self):

        concept.populate_concept_table(self.s)

        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k',
             'reported_ethnicities': [
                 {'ethnicity_code': 'A',
                  'source': 'dams',
                  'source_date': '2000-01-01'},
                 {'ethnicity_code': 'B',
                  'source': 'hes_apc',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'A',
                  'source': 'hes_apc',
                  'source_date': '1900-01-01'},
             ]}

        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        be = get_best_ethnicity_for_participant(d['id'])

        # this case should be A as most common (despite being oldest)
        self.assertEqual(be, 'A')

    def test_best_ethnicity_view_c(self):

        concept.populate_concept_table(self.s)

        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k',
             'reported_ethnicities': [
                 {'ethnicity_code': 'A',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'B',
                  'source': 'hes_apc',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'C',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
             ]}

        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        be = get_best_ethnicity_for_participant(d['id'])

        # this case should be B as all equally common and recent so take APC
        # more recent
        self.assertEqual(be, 'B')

    def test_best_ethnicity_view_d(self):

        concept.populate_concept_table(self.s)

        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k',
             'reported_ethnicities': [
                 {'ethnicity_code': 'H',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'C',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'J',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
             ]}

        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        be = get_best_ethnicity_for_participant(d['id'])

        # this case should be C as all equally common and recent from same
        # source but C most common in population
        self.assertEqual(be, 'C')

    def test_best_ethnicity_view_e(self):

        concept.populate_concept_table(self.s)

        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k',
             'reported_ethnicities': [
                 {'ethnicity_code': 'S',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'S',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'J',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
             ]}

        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        be = get_best_ethnicity_for_participant(d['id'])

        # this case should be J as is next most common non-other ethnic group
        self.assertEqual(be, 'J')

    def test_best_ethnicity_view_f(self):

        concept.populate_concept_table(self.s)

        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k',
             'reported_ethnicities': [
                 {'ethnicity_code': 'S',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'S',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'Z',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
             ]}

        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        be = get_best_ethnicity_for_participant(d['id'])

        # this case should be S as there are no other non-other ethnic groups
        # to choose from
        self.assertEqual(be, 'S')

    def test_best_ethnicity_view_g(self):

        concept.populate_concept_table(self.s)

        d = {'id': '1',
             'group': '100k_ca',
             'in_ngrl': True,
             'programme': '100k',
             'reported_ethnicities': [
                 {'ethnicity_code': '99',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'X',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
                 {'ethnicity_code': 'Z',
                  'source': 'dams',
                  'source_date': '1900-01-01'},
             ]}

        a = diversity_db.ObservedParticipant.from_dict(self.s, d)
        a.add_to_db(self.s)
        self.s.commit()

        be = get_best_ethnicity_for_participant(d['id'])

        # this case should be 99 as no valid ethnic group available
        self.assertEqual(be, '99')
