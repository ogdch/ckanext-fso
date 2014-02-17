#coding: utf-8

import os
import urllib3
from lxml import etree
from uuid import NAMESPACE_OID, uuid4, uuid5

from ckan import model
from ckan.model import Session, Package
from ckan.logic import get_action, action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name

from ckanext.harvest.model import HarvestObject
from base import OGDCHHarvesterBase

import logging
log = logging.getLogger(__name__)


class FSOHarvester(OGDCHHarvesterBase):
    '''
    The harvester for the FSO
    '''

    METADATA_URL = (
        "http://www.bfs.admin.ch/xmlns/opendata/BFS_OGD_metadata.xml"
    )
    FILES_BASE_URL = "http://www.bfs.admin.ch/xmlns/opendata/"
    HARVEST_USER = u'harvest'
    ORGANIZATION = {
        'de': {
            'name': u'Bundesamt für Statistik',
            'description': (
                u'Orientiert über den Stand und die Entwicklung der Schweiz '
                u'in zahlreichen Lebensbereichen. Es liefert die '
                u'quantitativen Informationen, um die Gegenwart zu '
                u'verstehen und die Zukunft zu planen.'
            ),
            'website': u'http://www.bfs.admin.ch/'
        },
        'fr': {
            'name': u'Office fédéral de la statistique',
            'description': (
                u'Fournit des informations sur l’état et l’évolution '
                u'de la Suisse dans de nombreux domaines. Les '
                u'informations qu’il produit servent à comprendre '
                u'le présent et à planifier l’avenir.'
            )
        },
        'it': {
            'name': u'Ufficio federale di statistica',
            'description': (
                u'Fornisce informazioni sullo stato e sull’evoluzione della '
                u'Svizzera nei più svariati settori che permettono di '
                u'capire il presente e pianificare il futuro.'
            )
        },
        'en': {
            'name': u'Swiss Federal Statistical Office',
            'description': (
                u'Publishes information on the situation and trends in '
                u'Switzerland in many different areas of life. It provides '
                u'the quantitative information needed to understand the '
                u'present and to plan for the future.'
            )
        }
    }
    GROUPS = {
        'de': [u'Bevölkerung', u'Politik', u'Statistische Grundlagen und Übersichten', u'Gesundheit'],
	'fr': [u'Population', u'Politique', u'Bases statistiques et généralités', u'Santé'],
	'it': [u'Popolazione', u'Politica', u'Basi statistiche e presentazioni generali', u'Salute'],
	'en': [u'Population', u'Politics', u'Statistical basis and overviews', u'Health']
    }
    NOTES_HELPERS = {
        'de': {
            'link_to_fso_population': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/01/01/keyw.html'
            ),
            'link_text_to_fso_population': (
                u'Das Thema Bevölkerung im Bundesamt für Statistik'
            ),
	    'link_to_fso_health': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/14/01/keyw.html'
            ),
            'link_text_to_fso_health': (
                u'Das Thema Gesundheit im Bundesamt für Statistik'
            ),
            'link_to_fso_basis': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/00.html'
            ),
            'link_text_to_fso_basis': (
                u'Das Thema Grundlagen und Übersichten im Bundesamt für Statistik'
            ),
            'link_to_fso_politics': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/17/01/keyw.html'
            ),
            'link_text_to_fso_politics': (
                u'Das Thema Politik im Bundesamt für Statistik'
            ),
            'inquiry_period': u'Periode der Erhebung'
        },
        'fr': {
            'link_to_fso_population': (
                u'http://www.bfs.admin.ch/bfs/portal/fr/index/themen'
                u'/01/01/keyw.html'
            ),
            'link_text_to_fso_population': (
                u"Le sujet de la population à l'Office "
                u"fédéral de la statistique"
            ),
	    'link_to_fso_health': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/14/01/keyw.html'
            ),
            'link_text_to_fso_health': (
                u'Das Thema Gesundheit im Bundesamt für Statistik'
            ),
	    'link_to_fso_basis': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/00.html'
            ),
            'link_text_to_fso_basis': (
                u'Das Thema Grundlagen und Übersichten im Bundesamt für Statistik'
            ),
            'link_to_fso_politics': (
                u'http://www.bfs.admin.ch/bfs/portal/fr/index/themen'
                u'/17/01/keyw.html'
            ),
            'link_text_to_fso_politics': (
                u"Le sujet de la politique à l'Office "
                u"fédéral de la statistique"
            ),
            'inquiry_period': u'Période de collection'
        },
        'it': {
            'link_to_fso_population': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/01/01/keyw.html'
            ),
            'link_text_to_fso_population': (
                u'Il tema della popolazione presso l\'Ufficio '
                u'federale di statistica'
            ),
	    'link_to_fso_health': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/14/01/keyw.html'
            ),
            'link_text_to_fso_health': (
                u'Das Thema Gesundheit im Bundesamt für Statistik'
            ),
	    'link_to_fso_basis': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/00.html'
            ),
            'link_text_to_fso_basis': (
                u'Das Thema Grundlagen und Übersichten im Bundesamt für Statistik'
            ),
            'link_to_fso_politics': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/17/01/keyw.html'
            ),
            'link_text_to_fso_politics': (
                u'Il tema della politica presso l\'Ufficio '
                u'federale di statistica'
            ),
            'inquiry_period': u'Periodo di inchiesta'
        },
        'en': {
            'link_to_fso_population': (
                u'http://www.bfs.admin.ch/bfs/portal/en/index/themen'
                u'/01/01/keyw.html'
            ),
            'link_text_to_fso_population': (
                u'The topic population at the Swiss Federal '
                u'Statistical Office'
            ),
	    'link_to_fso_health': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/14/01/keyw.html'
            ),
            'link_text_to_fso_health': (
                u'Das Thema Gesundheit im Bundesamt für Statistik'
            ),
	    'link_to_fso_basis': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/00.html'
            ),
            'link_text_to_fso_basis': (
                u'Das Thema Grundlagen und Übersichten im Bundesamt für Statistik'
            ),
            'link_to_fso_politics': (
                u'http://www.bfs.admin.ch/bfs/portal/de/index/themen'
                u'/17/01/keyw.html'
            ),
            'link_text_to_fso_politics': (
                u'The topic politics at the Swiss Federal '
                u'Statistical Office'
            ),
            'inquiry_period': u'Inquiry period'
        }
    }
    PUBLISHED_AT = {
        'de': u'Veröffentlicht:',
        'fr': u'Publié:',
        'it': u'Pubblicato:',
        'en': u'Published:'
    }

    config = {
        'user': u'admin'
    }

    def _create_uuid(self, name=None):
        '''
        Create a new SHA-1 uuid for a given name or a random id
        '''
        if name:
            new_uuid = uuid5(NAMESPACE_OID, str(name))
        else:
            new_uuid = uuid4()

        return unicode(new_uuid)

    def _gen_new_name(self, title, current_id=None):
        '''
        Creates a URL friendly name from a title

        If the name already exists, it will add some
        random characters at the end
        '''

        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        pkg_obj = Session.query(Package).filter(Package.name == name).first()
        if pkg_obj and pkg_obj.id != current_id:
            return name + str(uuid4())[:5]
        else:
            return name

    def _file_is_available(self, url):
        '''
        Returns true if 200, False otherwise. (logs falses)
        '''
        status = urllib3.PoolManager().request('HEAD', url).status
        if status == 200:
            return True
        else:
            log.debug(str(status) + ': ' + url)
            return False

    def _generate_tags_array(self, dataset):
        '''
        All tags for a dataset into an array
        '''
        tags = []
        for tag in dataset.find('tags').findall('tag'):
            tags.append(tag.text)
        return tags

    def _get_dataset_group(self, dataset):
        '''
        Get group name based on the policy discussed with the FSO
        '''
        for group_tag in dataset.find('groups').findall('group'):
            if group_tag.text[0:2] == "01":
                return self.GROUPS['de'][0]
            if group_tag.text[0:2] == "17":
                return self.GROUPS['de'][1]
	    if group_tag.text[0:2] == "00":
                return self.GROUPS['de'][2]
	    if group_tag.text[0:2] == "14":
                return self.GROUPS['de'][3]
        return None

    def _generate_notes(self, dataset, key):
        '''
        Concatenates all the notes pieces together into a single notes string
        '''
        if dataset.find('notes').text:
            notes = dataset.find('notes').text
        else:
            notes = ''

        if dataset.find('coverage').text:
            notes += (
                '\n  ' +
                self.NOTES_HELPERS[key]['inquiry_period'] + ' ' +
                dataset.find('coverage').text
            )

        # Published At -> Notes
        if dataset.find('published').text:
            notes += (
                '\n  ' +
                self.PUBLISHED_AT[key] + ' ' +
                dataset.find('published').text
            )

        # More Information -> Notes
        if dataset.find('groups').find('group').text[0:2] == "01":
            notes += (
                '\n  ' +
                "[" + self.NOTES_HELPERS[key]['link_text_to_fso_population'] +
                "](" + self.NOTES_HELPERS[key]['link_to_fso_population'] + ")"
            )

        if dataset.find('groups').find('group').text[0:2] == "17":
            notes += (
                '\n  ' +
                "[" + self.NOTES_HELPERS[key]['link_text_to_fso_politics'] +
                "](" + self.NOTES_HELPERS[key]['link_to_fso_politics'] + ")"
            )
        
	if dataset.find('groups').find('group').text[0:2] == "00":
            notes += (
                '\n  ' +
                "[" + self.NOTES_HELPERS[key]['link_text_to_fso_basis'] +
                "](" + self.NOTES_HELPERS[key]['link_to_fso_basis'] + ")"
            )

        if dataset.find('groups').find('group').text[0:2] == "14":
            notes += (
                '\n  ' +
                "[" + self.NOTES_HELPERS[key]['link_text_to_fso_health'] +
                "](" + self.NOTES_HELPERS[key]['link_to_fso_health'] + ")"
            )			
        else:
            log.debug(dataset.find('groups').find('group').text[0:2])

        return notes

    def _generate_term_translations(self, base_dataset, package):
        '''
        Return all the term_translations for a given dataset
        '''
        translations = []

        # term translations for the groups (finite set)
        for key, lang in self.GROUPS.iteritems():
            for idx, group in enumerate(self.GROUPS[key]):
                translations.append({
                    'lang_code': key,
                    'term': self.GROUPS['de'][idx],
                    'term_translation': group
                    })

        for lang, org in self.ORGANIZATION.items():
            if lang != 'de':
                for field in ['name', 'description']:
                    translations.append({
                        'lang_code': lang,
                        'term': self.ORGANIZATION['de'][field],
                        'term_translation': org[field]
                    })

        for dataset in package:
            if base_dataset.get('datasetID') != dataset.get('datasetID'):
                lang = dataset.get(
                    '{http://www.w3.org/XML/1998/namespace}lang'
                )
                keys = ['title', 'author', 'maintainer']
                for key in keys:
                    if base_dataset.find(key).text and dataset.find(key).text:
                        translations.append({
                            'lang_code': lang,
                            'term': base_dataset.find(key).text,
                            'term_translation': dataset.find(key).text
                            })

                for lang in self.NOTES_HELPERS:
                    if lang != 'de':
                        base_notes_translation = self._generate_notes(
                            base_dataset,
                            'de'
                        )
                        other_notes_translation = self._generate_notes(
                            dataset,
                            lang
                        )
                        translations.append({
                            'lang_code': lang,
                            'term': base_notes_translation,
                            'term_translation': other_notes_translation
                            })

        return translations

    def _generate_resources(self, package):
        '''
        Return all resources for a given package
        that return a HTTP Status of 200
        '''
        resources = []
        for dataset in package:
            resource_url = dataset.find('resource').find('url').text
            resource_name = dataset.find('resource').find('name').text
            if self._file_is_available(resource_url):
                resources.append({
                    'url': dataset.find('resource').find('url').text,
                    'name': dataset.find('resource').find('name').text,
                    'format': self._guess_format(resource_name)
                    })
        return resources

    def _guess_format(self, file_name):
            '''
            Return the format for a given full filename
            '''
            _, file_extension = os.path.splitext(file_name.lower())
            return file_extension[1:]

    def _generate_metadata(self, base_dataset, package):
        '''
        Return all the necessary metadata to be able to create a dataset
        '''
        resources = self._generate_resources(package)
        group = self._get_dataset_group(base_dataset)

        if len(resources) != 0 and group:
            translations = self._generate_term_translations(
                base_dataset,
                package
            )
            return {
                'datasetID': base_dataset.get('datasetID'),
                'title': base_dataset.find('title').text,
                'notes': self._generate_notes(base_dataset, 'de'),
                'author': base_dataset.find('author').text,
                'maintainer': base_dataset.find('maintainer').text,
                'maintainer_email': base_dataset.find('maintainer_email').text,
                'license_url': base_dataset.find('licence').text,
                'license_id': base_dataset.find('copyright').text,
                'translations': translations,
                'resources': resources,
                'tags': self._generate_tags_array(base_dataset),
                'groups': [group]
            }
        else:
            return None

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
        metadata_file = http.request('GET', self.METADATA_URL)

        ids = []
        parser = etree.XMLParser(encoding='utf-8')
        for package in etree.fromstring(metadata_file.data, parser=parser):

            # Get the german dataset if one is available
            # otherwise get the first one
            base_datasets = package.xpath("dataset[@xml:lang='de']")
            if len(base_datasets) != 0:
                base_dataset = base_datasets[0]
            else:
                base_dataset = package.find('dataset')

            metadata = self._generate_metadata(base_dataset, package)
            if metadata:
                obj = HarvestObject(
                    guid=self._create_uuid(base_dataset.get('datasetID')),
                    job=harvest_job,
                    content=json.dumps(metadata)
                )
                obj.save()
                log.debug(
                    'adding %s to the queue' % base_dataset.get('datasetID')
                )
                ids.append(obj.id)
            else:
                log.debug(
                    'Skipping %s since no resources or groups are available'
                    % base_dataset.get('datasetID')
                )

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
            raise

    def import_stage(self, harvest_object):
        log.debug('In FSOHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            package_dict = json.loads(harvest_object.content)

            package_dict['id'] = harvest_object.guid
            package_dict['name'] = self._gen_new_name(
                package_dict['datasetID'],
                package_dict['id']
            )

            user = model.User.get(self.HARVEST_USER)
            context = {
                'model': model,
                'session': Session,
                'user': self.HARVEST_USER
                }

            # Find or create group the dataset should get assigned to
            for group_name in package_dict['groups']:
                if not group_name:
                    raise GroupNotFoundError(
                        'Group is not defined for dataset %s'
                        % package_dict['title']
                    )
                data_dict = {
                    'id': group_name,
                    'name': munge_title_to_name(group_name),
                    'title': group_name
                    }
                try:
                    group = get_action('group_show')(context, data_dict)
                    log.info('found the group %s' % group['id'])
                except:
                    group = get_action('group_create')(context, data_dict)
                    log.info('created the group ' + group['id'])

            # Find or create the organization
            # the dataset should get assigned to
            try:
                name = munge_title_to_name(self.ORGANIZATION['de']['name'])
                data_dict = {
                    'permission': 'edit_group',
                    'id': name,
                    'name': name,
                    'title': self.ORGANIZATION['de']['name'],
                    'description': self.ORGANIZATION['de']['description'],
                    'extras': [
                        {
                            'key': 'website',
                            'value': self.ORGANIZATION['de']['website']
                        }
                    ]
                }
                org = get_action('organization_show')(context, data_dict)
                package_dict['owner_org'] = org['id']
            except:
                organization = get_action('organization_create')(
                    context,
                    data_dict
                )
                package_dict['owner_org'] = organization['id']

            # Save additional metadata in extras
            extras = []
            if 'license_url' in package_dict:
                extras.append(('license_url', package_dict['license_url']))
            package_dict['extras'] = extras
            log.debug('Extras %s' % extras)

            package = model.Package.get(package_dict['id'])
            model.PackageRole(
                package=package,
                user=user,
                role=model.Role.ADMIN
            )

            self._create_or_update_package(package_dict, harvest_object)

            # Add the translations to the term_translations table
            for translation in package_dict['translations']:
                action.update.term_translation_update(context, translation)
            Session.commit()

        except Exception, e:
            log.exception(e)
            raise
        return True


class GroupNotFoundError(Exception):
    pass
