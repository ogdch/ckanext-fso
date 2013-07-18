#coding: utf-8

import urllib3
from lxml import etree

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
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
            base_datasets = package.xpath("dataset[@xml:lang='de']")
            if len(base_datasets) != 0:
                base_dataset = base_datasets[0]
            else:
                base_dataset = package.find('dataset')

            dataset_id = base_dataset.get('datasetID')

            metadata = {
                'datasetID': dataset_id,
                'title': base_dataset.find('title').text,
                'notes': base_dataset.find('notes').text,
                'author': base_dataset.find('author').text,
                'maintainer': base_dataset.find('maintainer').text,
                'maintainer_email': base_dataset.find('maintainer_email').text,
                'translations': [],
                'resources': [],
                'tags': []
            }

            # Assinging tags to the dataset
            for tag in base_dataset.find('tags').findall('tag'):
                metadata['tags'].append(tag.text)


            for dataset in package:                
                if dataset.get('datasetID') != base_dataset.get('datasetID'):
                    # Adding resources to the dataset
                    metadata['resources'].append({
                        'url': dataset.find('resource').find('url').text,
                        'name': dataset.find('resource').find('name').text,
                        'format': 'XLS'
                        })

                    # Adding term translations to the metadata
                    keys = ['title', 'notes', 'author', 'maintainer']
                    for key in keys:
                        if base_dataset.find(key).text and dataset.find(key).text:
                            metadata['translations'].append({
                                'lang_code': dataset.get('{http://www.w3.org/XML/1998/namespace}lang'),
                                'term': base_dataset.find(key).text,
                                'term_translation': dataset.find(key).text
                                })

                    # tags term translations (NEEDS TO BE ENABLED LATER WHEN THE METADATA IS CORRECT)
                    # for idx, tag in enumerate(dataset.find('tags').iter('tag')):
                    #     if tag.text and metadata['tags'][idx]:
                    #         metadata['translations'].append({
                    #             'lang_code': dataset.get('{http://www.w3.org/XML/1998/namespace}lang'),
                    #             'term': metadata['tags'][idx],
                    #             'term_translation': tag.text
                    #             })


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
            package_dict = json.loads(harvest_object.content)

            package_dict['id'] = harvest_object.guid
            package_dict['name'] = self._gen_new_name(package_dict['title'])

            user = model.User.get(u'admin')
            package = model.Package.get(package_dict['id'])
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            result = self._create_or_update_package(package_dict, harvest_object)

            # Add the translations to the term_translations table
            for translation in package_dict['translations']:
                context = {
                    'model': model,
                    'session': Session,
                    'user': u'admin'
                }
                action.update.term_translation_update(context, translation)
            Session.commit()

        except Exception, e:
            log.exception(e)
        return True
