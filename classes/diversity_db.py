"""
provides the classes for interacting the database store
"""

import logging
from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Integer,\
    Numeric, String, Table, Text, UniqueConstraint, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()
LOGGER = logging.getLogger(__name__)


class Concept(BASE):
    """
    the SQLAlchemy class for concept data
    stores all enumerations used in the database
    """

    __tablename__ = 'concept'
    __table_args__ = ({'schema': 'ethnicity_store'})

    uid = Column(UUID, primary_key=True,
                 server_default=text("uuid_generate_v4()"))
    concept_code = Column(String, nullable=False)
    codesystem = Column(String, nullable=False)
    description = Column(String, nullable=False)


class ReportedEthnicity(BASE):
    """
    the SQLAlchemy class for reported_ethnicity
    stores all instances of reported ethnicity found against each participant
    """

    __tablename__ = 'reported_ethnicity'
    __table_args__ = ({'schema': 'ethnicity_store'})

    participant_id = Column(String, ForeignKey('ethnicity_store.participant.id'),
                            nullable=False, primary_key=True)
    ethnicity_cid = Column(UUID, ForeignKey('ethnicity_store.concept.uid'),
                           nullable=False, primary_key=True)
    source_cid = Column(UUID, ForeignKey('ethnicity_store.concept.uid'),
                        nullable=False, primary_key=True)
    source_date = Column(Date, nullable=False, primary_key=True)


class Participant(BASE):
    """
    the SQLAlchemy class for participant
    stores all participants and metadata
    """

    __tablename__ = 'participant'
    __table_args__ = ({'schema': 'ethnicity_store'})

    id = Column(String, primary_key=True, nullable=False)
    group_cid = Column(UUID, ForeignKey('ethnicity_store.concept.uid'),
                       nullable=False)
    in_ngrl = Column(Boolean, default=False)
    programme_cid = Column(UUID, ForeignKey('ethnicity_store.concept.uid'),
                           nullable=False)


class ObservedParticipant(Participant):
    """
    the class for participants observed in our data
    provides methods to transition data from the sources into the database
    """

    # attributes to hold dictionaries of concept_code: uid so that observed
    # groups and programmes can be converted into group_cids for loading
    group_concept_codes = None
    programme_concept_codes = None

    # attribute to hold all participants already in database
    participants_in_db = None

    def __init__(self, s, id, group, in_ngrl, programme,
                 reported_ethnicities=[]):
        """
        create new ObservedParticipant
        :params s: SQLAlchemy session bound to required engines
        :params id: id of participant
        :params group: group participant is a member of
        :params in_ngrl: boolean, is participants in ngrl?
        :params programme: participant programme membership
        :params reported_ethnicities: list of dictionaries giving reported
        ethnicity information
        """

        LOGGER.debug(f'creating new ObservedParticipant for {id}')

        # if we don't have any of the required reference data
        # then populate them from the db
        if not self.group_concept_codes:

            q = s.query(Concept.concept_code,
                        Concept.uid).\
                filter(Concept.codesystem == 'group')

            ObservedParticipant.group_concept_codes = \
                {x[0]: x[1] for x in q}

            assert len(ObservedParticipant.group_concept_codes) > 0

        if not self.programme_concept_codes:

            q = s.query(Concept.concept_code,
                        Concept.uid).\
                filter(Concept.codesystem == 'programme')

            ObservedParticipant.programme_concept_codes = \
                {x[0]: x[1] for x in q}

            assert len(ObservedParticipant.programme_concept_codes) > 0

        if not self.participants_in_db:

            q = s.query(Participant)

            ObservedParticipant.participants_in_db = {x.id: x for x in q.all()}

        self.id = id
        self.group_cid = self.group_concept_codes[group]
        self.in_ngrl = in_ngrl
        self.programme_cid = self.programme_concept_codes[programme]

        # loop through each of the reported ethnicities and create instances
        # of observed reported ethnicities
        self.reported_ethnicities = [
            ObservedReportedEthnicity.from_dict(s, {'participant_id':
                                                    self.id, **x})
            for x in reported_ethnicities]

    def add_to_db(self, s):
        """
        add the participant and any reported ethnicity data to the database
        session and flush it through
        :params s: SQLAlchemy session bound to required engines
        """

        # check whether participant is in database so either merge or add
        if self.id in ObservedParticipant.participants_in_db.keys():

            s.merge(self)

        else:

            s.add(self)

        for x in self.reported_ethnicities:

            x.add_to_db(s)

        s.flush()

    def __repr__(self):

        return f'<Participant {self.id}>'

    @classmethod
    def from_dict(cls, s, d):
        """
        allows creation from a dictionary
        :params s: SQLAlchemy session bound to required engines
        :params d: dictionary with the required keys and values
        :returns: instance of ObservedParticipant
        """

        return cls(s, **d)


class ObservedReportedEthnicity(ReportedEthnicity):
    """
    the class for reported ethnicity observed in our data
    provides methods to transition data from the sources to the database
    """

    # attribute to hold a dictionary of ethnicity concept_code: uid
    # so that observed ethnicities can be converted into ethnicity_cids for
    # loading
    ethnicity_concept_codes = None
    # attribute to hold a dictionary of source concept_code: uid so that sources
    # can be converted into source_cids for loading
    source_concept_codes = None

    def __init__(self, s, participant_id, ethnicity_code, source, source_date):
        """
        create an instance of ObservedReportedEthnicity
        :params s: SQLAlchemy session bound to required engines
        :params participant_id: participant ID
        :params ethnicity_code: the single letter code for ethnicity
        :params source: the code for the data source
        :params source_date: the date for the data point
        """

        LOGGER.debug('creating new instance of ObservedReportedEthnicity')

        # if we don't have ethnicity_concept_codes or source_concept_codes
        # populate them from the db
        if not self.ethnicity_concept_codes:

            q = s.query(Concept.concept_code,
                        Concept.uid).\
                filter(Concept.codesystem == 'reported_ethnicity_code')

            ObservedReportedEthnicity.ethnicity_concept_codes = \
                {x[0]: x[1] for x in q}

            assert len(ObservedReportedEthnicity.ethnicity_concept_codes) > 0

        if not self.source_concept_codes:

            q = s.query(Concept.concept_code,
                        Concept.uid).\
                filter(Concept.codesystem == 'source')

            ObservedReportedEthnicity.source_concept_codes = \
                {x[0]: x[1] for x in q}

            assert len(ObservedReportedEthnicity.source_concept_codes) > 0

        self.participant_id = participant_id
        self.ethnicity_code = ethnicity_code
        self.ethnicity_cid = self.ethnicity_concept_codes.get(ethnicity_code)
        self.source = source
        self.source_cid = self.source_concept_codes.get(source)
        self.source_date = source_date

    def add_to_db(self, s):
        """
        merge the object into the database and flush it through
        :params s: SQLAlchemy session bound to required engines
        """

        s.merge(self)
        s.flush()

    def __repr__(self):

        return f'<Reported Ethnicity {self.participant_id}:{self.ethnicity_code}>'

    @classmethod
    def from_dict(cls, s, d):
        """
        allows creation from a dictionary
        :params s: SQLAlchemy session bound to required engines
        :params d: dictionary with the required keys and values
        :returns: instance of ObservedReportedEthnicity
        """

        return cls(s, **d)
