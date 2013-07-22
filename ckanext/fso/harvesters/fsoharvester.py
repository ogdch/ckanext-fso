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
    HARVEST_USER = u'harvest'
    GROUPS = {
        'de': [u'Bevölkerung', u'Politik'],
        'fr': [u'Population', u'Politique'],
        'it': [u'Popolazione', u'Politica'],
        'en': [u'Population', u'Politics']
    }
    NOTES_HELPERS = {
        'de': {
            'link_to_fso_population': u'http://www.bfs.admin.ch/bfs/portal/de/index/themen/01/01/keyw.html',
            'link_text_to_fso_population': u'Das Thema Bevölkerung im Bundesamt für Statistik',
            'link_to_fso_politics': u'http://www.bfs.admin.ch/bfs/portal/de/index/themen/17/01/keyw.html',
            'link_text_to_fso_politics': u'Das Thema Politik im Bundesamt für Statistik',
            'inquiry_period': u'Periode der Erhebung'
        },
        'fr': {
            'link_to_fso_population': u'http://www.bfs.admin.ch/bfs/portal/fr/index/themen/01/01/keyw.html',
            'link_text_to_fso_population': u"Le sujet de la population à l'Office fédéral de la statistique",
            'link_to_fso_politics': u'http://www.bfs.admin.ch/bfs/portal/de/index/themen/17/01/keyw.html',
            'link_text_to_fso_politics': u"Le sujet de la politique à l'Office fédéral de la statistique",
            'inquiry_period': u'Période de collection'
        },
        'it': {
            'link_to_fso_population': u'it_http://www.bfs.admin.ch/bfs/portal/de/index/themen/01/01/keyw.html',
            'link_text_to_fso_population': u'it_Das Thema Bevölkerung im Bundesamt für Statistik',
            'link_to_fso_politics': u'it_http://www.bfs.admin.ch/bfs/portal/de/index/themen/17/01/keyw.html',
            'link_text_to_fso_politics': u'it_Das Thema Politik im Bundesamt für Statistik',
            'inquiry_period': u'it_Periode der Erhebung'
        },
        'en': {
            'link_to_fso_population': u'http://www.bfs.admin.ch/bfs/portal/en/index/themen/01/01/keyw.html',
            'link_text_to_fso_population': u'en_The topic population at the Swiss Federal Statistical Office',
            'link_to_fso_politics': u'en_http://www.bfs.admin.ch/bfs/portal/de/index/themen/17/01/keyw.html',
            'link_text_to_fso_politics': u'en_The topic politics at the Swiss Federal Statistical Office',
            'inquiry_period': u'en_Inquiry period'
        }
    }
    PUBLISHED_AT = {
        'de': u'Veröffentlicht:',
        'fr': u'fr_Veröffentlicht:',
        'it': u'it_Veröffentlicht:',
        'en': u'Published:'
    }

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
        parser = etree.XMLParser(encoding='utf-8')
        for package in etree.fromstring(metadata_file.data, parser=parser):

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
                'license_id': base_dataset.find('licence').text,
                'translations': [],
                'resources': [],
                'tags': [],
                'groups': []
            }

            # Assinging tags to the dataset
            for tag in base_dataset.find('tags').findall('tag'):
                metadata['tags'].append(tag.text)

            # Assigning a group to the dataset
            if base_dataset.find('groups').find('group').text[0:2] == "01":
                metadata['groups'].append(self.GROUPS['de'][0])
            elif base_dataset.find('groups').find('group').text[0:2] == "17":
                metadata['groups'].append(self.GROUPS['de'][1])

            # Assigning notes additions
            if base_dataset.find('notes').text == None:
                metadata['notes'] = ''
            if base_dataset.find('coverage').text:
                metadata['notes'] += '\n  ' + self.NOTES_HELPERS['de']['inquiry_period'] + ' ' + base_dataset.find('coverage').text

            # Published At -> Notes
            if base_dataset.find('published').text:
                metadata['notes'] += '\n  ' + self.PUBLISHED_AT['de'] + ' ' + base_dataset.find('published').text

            # More Information -> Notes
            if base_dataset.find('groups').find('group').text[0:2] == "01":
                metadata['notes'] += '\n  ' + "[" + self.NOTES_HELPERS['de']['link_text_to_fso_population'] +\
                "](" + self.NOTES_HELPERS['de']['link_to_fso_population'] + ")"

            elif base_dataset.find('groups').find('group').text[0:2] == "17":
                metadata['notes'] += '\n  ' + "[" + self.NOTES_HELPERS['de']['link_text_to_fso_politics'] +\
                "](" + self.NOTES_HELPERS['de']['link_to_fso_politics'] + ")"
            else:
                log.debug(base_dataset.find('groups').find('group').text[0:2])

            # Adding term translations for the groups
            for key, lang in self.GROUPS.iteritems():
                for idx, group in enumerate(self.GROUPS[key]):
                    metadata['translations'].append({
                        'lang_code': key,
                        'term': self.GROUPS['de'][idx],
                        'term_translation': group
                        })


            for dataset in package:
                
                # Adding resources to the dataset
                metadata['resources'].append({
                    'url': dataset.find('resource').find('url').text,
                    'name': dataset.find('resource').find('name').text,
                    'format': 'XLS'
                    })

                if dataset.get('datasetID') != base_dataset.get('datasetID'):
                    lang = dataset.get('{http://www.w3.org/XML/1998/namespace}lang')

                    # Adding term translations to the metadata
                    keys = ['title', 'author', 'maintainer']
                    for key in keys:
                        if base_dataset.find(key).text and dataset.find(key).text:
                            metadata['translations'].append({
                                'lang_code': lang,
                                'term': base_dataset.find(key).text,
                                'term_translation': dataset.find(key).text
                                })

                    # Adding term translations for notes
                    notes_term_translation = ''
                    if base_dataset.find('notes').text and dataset.find('notes').text:
                        notes_term_translation = dataset.find('notes').text

                    if base_dataset.find('coverage').text:
                        log.debug(base_dataset.find('coverage').text)

                        notes_term_translation += '\n  ' + self.NOTES_HELPERS[lang]['inquiry_period'] + ' ' +\
                        base_dataset.find('coverage').text

                    # Published At -> Notes
                    if base_dataset.find('published').text:
                        notes_term_translation += '\n ' + self.PUBLISHED_AT['de'] + ' ' + base_dataset.find('published').text

                    # More Information -> Notes
                    if base_dataset.find('groups').find('group').text[0:2] == "01":
                        notes_term_translation += '\n  ' + "[" + self.NOTES_HELPERS[lang]['link_text_to_fso_population'] +\
                        "](" + self.NOTES_HELPERS[lang]['link_to_fso_population'] + ")"

                    elif base_dataset.find('groups').find('group').text[0:2] == "17":
                        notes_term_translation += '\n  ' + "[" + self.NOTES_HELPERS[lang]['link_text_to_fso_politics'] +\
                        "](" + self.NOTES_HELPERS[lang]['link_to_fso_politics'] + ")"

                    metadata['translations'].append({
                        'lang_code': lang,
                        'term': metadata['notes'],
                        'term_translation': notes_term_translation
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
            package_dict = json.loads(harvest_object.content)

            package_dict['id'] = harvest_object.guid
            package_dict['name'] = self._gen_new_name(package_dict['title'])

            user = model.User.get(self.HARVEST_USER)
            context = {
                'model': model,
                'session': Session,
                'user': self.HARVEST_USER
                }

            for group_name in package_dict['groups']:
                try:
                    data_dict = {
                        'id': group_name,
                        'name': self._gen_new_name(group_name),
                        'title': group_name
                        }
                    group_id = get_action('group_show')(context, data_dict)['id']
                except:
                    group = get_action('group_create')(context, data_dict)
                    log.info('created the group ' + group['id'])

            package = model.Package.get(package_dict['id'])
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            result = self._create_or_update_package(package_dict, harvest_object)

            # Add the translations to the term_translations table
            for translation in package_dict['translations']:
                action.update.term_translation_update(context, translation)
            Session.commit()

        except Exception, e:
            log.exception(e)
        return True
