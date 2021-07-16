"""
provides interface to process ethnicity
"""

import logging
import fire
from config import ConfigFactory
from modules import log, database, concept
from classes import diversity_db

c = ConfigFactory.factory()
log.setup_logger(c)

LOGGER = logging.getLogger(__name__)


class Diversity:

    def create_diversity_db(self):

        database.create_diversity_db(c)
        s = database.make_session(c)
        concept.populate_concept_table(s)

    def drop_diversity_db(self):

        database.drop_diversity_db(c)


if __name__ == "__main__":
    """
    build the fire interface
    """
    fire.Fire(Diversity)
