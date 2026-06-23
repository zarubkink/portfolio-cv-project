from src.models.base import BaseFields
from src.schemas.station import StationBase


class Station(BaseFields, StationBase, table=True):
    __tablename__ = "stations"
