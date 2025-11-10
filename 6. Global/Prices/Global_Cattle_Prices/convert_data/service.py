from .cwt_per_usd import Cwt_per_usd
from .usd_per_at import Usd_per_at

def convert_data(path: str):
    Cwt_per_usd.convert_cwt_per_usd(path)
    Usd_per_at.convert_usd_per_at(path)