from google.appengine.ext import vendor

vendor.add('lib')

from googleapiclient import discovery
from oauth2client import client

discovery.logger.setLevel('WARNING')
client.logger.setLevel('WARNING')
