from .Uruguay import Uruguay
from .Europe import Europe
from .United_States import United_States
from .Canada import Canada
from .Argentina import Argentina
from .Great_Britain import Great_Britain


def organize_data(path: str, all_dates: dict):
    Uruguay.organize_Prices(path)
    # Europe.organize_Prices(path, all_dates)
    United_States.organize_Prices(path)
    Argentina.organize_Prices(path)
    Great_Britain.organize_Prices(path)
    Canada.organize_Prices(path)
