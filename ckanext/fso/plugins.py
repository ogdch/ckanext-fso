import ckan
import ckan.plugins as p
from pylons import config

class FsoHarvest(p.SingletonPlugin):
    """
    Plugin containg the harvester for FSO
    """
