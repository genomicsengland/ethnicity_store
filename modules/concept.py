"""
functions for working with the concept data
"""

import csv
import logging
from classes.diversity_db import Concept

CONCEPT_DATA_FP = 'resources/concept_codes.csv'

LOGGER = logging.getLogger(__name__)

def populate_concept_table(s):
    """
    write all the concept data to the concept table
    :params s: SQLAlchemy session bound to required engine
    """

    with open(CONCEPT_DATA_FP, "r") as f:

        csv_reader = csv.DictReader(f, delimiter=',')
        line_count = 0

        for row in csv_reader:

            create_concept_row(s, row['concept_code'],
                                row['codesystem'], row['description'])

            line_count += 1

        s.commit()

    LOGGER.info(f'Added {line_count} rows to concept table')


def create_concept_row(s, concept_code, codesystem, description=None):
    """
    create a new row in the concept table
    """

    a = Concept(
        concept_code=concept_code,
        codesystem=codesystem,
        description=description
    )

    s.add(a)
    s.flush()
