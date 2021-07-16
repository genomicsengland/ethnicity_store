"""
test the basic operation of diversity_db
"""

import logging
import unittest
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



