import ckan
import ckan.plugins as p
from pylons import config

class FsoHarvest(p.SingletonPlugin):
    """
    Plugin containing the harvester for FSO
    """
