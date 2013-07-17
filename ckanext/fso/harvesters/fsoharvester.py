#coding: utf-8

import urllib3
from lxml import etree

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from ckanext.harvest.harvesters import HarvesterBase

import logging
log = logging.getLogger(__name__)

class FSOHarvester(HarvesterBase):
    '''
    The harvester for the FSO
    '''

    METADATA_FILE_URL = "http://www.bfs.admin.ch/xmlns/opendata/BFS_OGD_metadata.xml"
    FILES_BASE_URL = "http://www.bfs.admin.ch/xmlns/opendata/"

    config = {
        'user': u'admin'
    }

    def info(self):
        return {
            'name': 'fso',
            'title': 'FSO',
            'description': 'Harvests the FSO data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In FSOHarvester gather_stage')

        http = urllib3.PoolManager()
        metadata_file = http.request('GET', self.METADATA_FILE_URL)

        ids = []
        for package in etree.fromstring(metadata_file.data):

            # Get the german dataset if one is available, otherwise get the first one
            datasets = package.xpath("dataset[@xml:lang='de']")
            if len(datasets) != 0:
                dataset = datasets[0]
            else:
                dataset = package.find('dataset')

            dataset_id = dataset.get('datasetID')

            metadata = {
                'datasetID': dataset_id,
                'title': dataset.find('title').text,
                'resources': []
            }

            metadata['resources'].append({
                'url': dataset.find('resource').find('url').text,
                'name': dataset.find('resource').find('name').text,
                'format': 'XLS'
                })

            obj = HarvestObject(
                guid = dataset_id,
                job = harvest_job,
                content = json.dumps(metadata)
            )
            obj.save()
            log.debug('adding ' + dataset_id + ' to the queue')
            ids.append(obj.id)

        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In FSOHarvester fetch_stage')

        # Get the URL
        datasetID = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        # Get contents
        try:
            harvest_object.save()
            log.debug('successfully processed ' + datasetID)
            return True
        except Exception, e:
            log.exception(e)

    def import_stage(self, harvest_object):
        log.debug('In FSOHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            metadata = json.loads(harvest_object.content)

            package_dict = {
                'title': metadata['title'],
                'notes': 'some description',
                'author': 'some author',
                'maintainer': 'some maintainer',
                'maintainer_email': 'ogd@liip.ch',
                'resources': []
            }

            for resource in metadata['resources']:
                package_dict['resources'].append(resource)

            package_dict['id'] = harvest_object.guid
            package_dict['name'] = self._gen_new_name(package_dict['title'])

            user = model.User.get(u'admin')
            package = model.Package.get(package_dict['id'])
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            result = self._create_or_update_package(package_dict, harvest_object)

        except Exception, e:
            log.exception(e)
        return True
