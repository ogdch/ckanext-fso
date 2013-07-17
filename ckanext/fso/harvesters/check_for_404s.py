import urllib3
from lxml import etree

METADATA_FILE_URL = "http://www.bfs.admin.ch/xmlns/opendata/BFS_OGD_metadata.xml"

# Fetch the metadata file once
http = urllib3.PoolManager()
metadata_file = http.request('GET', METADATA_FILE_URL)

# Create an output file
with open('missing_files.txt', 'w') as output_file:

	# Parse the metadata file
	for package in etree.fromstring(metadata_file.data):
		for dataset in package.findall('dataset'):
			resource = dataset.find('resource')
			url = resource.find('url').text

			request = http.request('GET', url)
			if request.status != 200:
				output_file.write(str(request.status) + ': ' + url + '\n')
				print str(request.status) + ': ' + url
