from .cartacapital import CartaCapitalHarvester
from .poder360 import Poder360Harvester
from .cnn import CNNHarvester
from .base import BaseHarvester


class HarvesterFactory:
    def __init__(self):
        # Map Domains -> Custom Classes
        self.specialists = {
            "CNN Brasil": CNNHarvester(),
            "Poder 360": Poder360Harvester(),
            "CartaCapital": CartaCapitalHarvester(),
            # Add "uol.com.br" here later IF they are weird.
        }
        self.generalist = BaseHarvester()

    def get_harvester(self, newspaper: str) -> BaseHarvester:
        return self.specialists.get(newspaper, self.generalist)
