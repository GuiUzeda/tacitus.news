from .base import BaseHarvester
from .metropoles import MetropolesHarvester


class HarvesterFactory:
    def __init__(self):
        # Map Domains -> Custom Classes
        self.specialists = {
            "Metrópoles": MetropolesHarvester(),
        }
        self.generalist = BaseHarvester()

    def get_harvester(self, newspaper: str) -> BaseHarvester:
        return self.specialists.get(newspaper, self.generalist)
