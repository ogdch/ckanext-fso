ckanext-fso
===========

Harvester for the Swiss Federal Statistical Office (FSO)

## Installation

Use `pip` to install this plugin. This example installs it in `/home/www-data`

```bash
source /home/www-data/pyenv/bin/activate
pip install -e git+https://github.com/ogdch/ckanext-fso.git#egg=ckanext-fso --src /home/www-data
cd /home/www-data/ckanext-fso
pip install -r pip-requirements.txt
python setup.py develop
```

Make sure to add `fso` and `fso_harvester` to `ckan.plugins` in your config file.

### For development
* install the `pre-commit.sh` script as a pre-commit hook in your local repositories:
** `ln -s ../../pre-commit.sh .git/hooks/pre-commit`

## Run harvester

```bash
source /home/www-data/pyenv/bin/activate
paster --plugin=ckanext-fso harvester gather_consumer -c development.ini &
paster --plugin=ckanext-fso harvester fetch_consumer -c development.ini &
paster --plugin=ckanext-fso harvester run -c development.ini
```
