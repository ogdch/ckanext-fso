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
            for dataset in package:
                id = dataset.get('datasetID')
                obj = HarvestObject(
                    guid = id,
                    job = harvest_job,
                    content = json.dumps({
                        'datasetID': id,
                        'url': 'http://pkstudio.ch/data.json'
                    })
                )
                obj.save()
                log.debug('adding ' + id + ' to the queue')
                ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In FSOHarvester fetch_stage')

        # Get the URL
        url = json.loads(harvest_object.content)['url']
        datasetID = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        # Get contents
        http = urllib3.PoolManager()
        try:
            file = http.request('GET', url)
            # metadata = harvest_object.content
            # harvest_object.content = {
            #     'metadata': metadata,
            #     'file': file.read()
            # }
            harvest_object.content = file.read()
            harvest_object.save()
            log.debug('successfully downloaded and saved ' + datasetID)
            return True
        except Exception, e:
            log.exception(e)

    def import_stage(self, harvest_object):
        log.debug('In FSOHarvester import_stage')

        # context = {'model': model, 'user': c.user}
        # try:
        #     user = get_action('user_show')(context, {'id': 'harvest'})
        # except NotFound,e:
        #     log.error('User not found')


        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            log.error('No harvest object content received')
            return False

        try:
            package_dict = {
                'title': harvest_object.guid,
                'url': 'http://pkstudio.ch/data.json',
                'notes': 'some description',
                'author': 'some author',
                'maintainer': 'some maintainer',
                'maintainer_email': 'ogd@liip.ch',
                'resources': []
            }

            package_dict['resources'].append({
                'url': 'http://pkstudio.ch/data.json',
                'format': 'application/json',
                'description': 'a sample description for this resource'
                })

            package_dict['id'] = harvest_object.guid
            package_dict['name'] = self._gen_new_name(package_dict['title'])

            user = model.User.get(u'admin')
            package = model.Package.get(package_dict['id'])
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            result = self._create_or_update_package(package_dict, harvest_object)

            # dataset_url = json.loads(harvest_object.content['metadata'])['datasetID']
            # package_dict['name'] = json.loads(harvest_object.content['metadata'])['datasetID']

        except Exception, e:
            log.exception(e)
        return True
        # return self._create_or_update_package(package_dict, harvest_object)
