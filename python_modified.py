import urllib2
import json
from collections import namedtuple

token = ''

def url(path, query={}):
  filters = '&'.join('%s=%s' % (k, ','.join(map(str, v))) 
  for k, v in query.items())
  append = '?' + filters if filters else ''
  return 'https://api-test.appnexus.com/%s%s' % (path, append)

def reauth():
  body = json.dumps({'auth': {'username': 'cxtdev@betgenius.com', 'password': 'C0nnextr@123'}})

  req = urllib2.Request(url=url('auth'), data=body)
  response = urllib2.urlopen(req)

  token = json.load(response)['response']['token']
  with open('token', 'w') as file:
    file.write(token)

  print ('created new token')
  return token

try:
  with open('token', 'r') as file:
      token = file.read()
  print ('got cached token')
except IOError:
  token = reauth()

Entry = namedtuple('Entry', ['endpoint', 'output_file', 'object_field', 'columns'])
Column = namedtuple('Column', ['name', 'fields'])

requests = [
      # Entry('operating-system-extended', 'os', 'operating-systems-extended', [
      #     Column('id', ['id']),
      #     Column('name', ['family', 'name'])
      # ]),
      # Entry('city', 'cities', 'cities', [
      #     Column('id', ['id']),
      #     Column('name', ['name']),
      #     Column('country', ['country_name']),
      #     Column('region', ['region_name']),
      #     Column('dma', ['dma_name'])
      # ]),
      Entry('campaign', 'campaigns', 'campaigns', [
          Column('id', ['id']),
          Column('name', ['name']),
          Column('state', ['state'])
      ]),
      # Entry('advertiser', 'advertisers', 'advertisers', [
      #     Column('id', ['id']),
      #     Column('name', ['name'])
      # ]),
      Entry('creative', 'creatives', 'creatives', [
          Column('id', ['id']),
          Column('name', ['name']),
          Column('state', ['state'])
      ]), 
      Entry('browser', 'browsers', 'browsers', [
          Column('id', ['id']),
          Column('name', ['name'])
      ]),
      # Entry('dma', 'dmas', 'dmas', [
      #     Column('id', ['id']),
      #     Column('name', ['name'])
      # ])
]

def transform(obj, columns):
  record = []
  for name, fields in columns:
      temp = obj
      for field in fields:
          temp = temp[field]
      data = str(temp) if isinstance(temp, int) else temp.encode('utf-8')
      record.append(data)
  return record

for entry in requests:

  print 'fetching %s' % entry.output_file

  min_id = 0

  with open(entry.output_file, 'w') as outfile:
    headers = '\t'.join(col.name for col in entry.columns)
    outfile.write(headers + '\n')

    while True:
      params = {
          'min_id': [min_id],
          'fields': [col.fields[0] for col in entry.columns]
      }
      req = urllib2.Request(url=url(entry.endpoint, params), headers={'Authorization': token})
      response = urllib2.urlopen(req)

      if response.getcode() == 401:
          reauth()
          continue

      oss = json.load(response)

      if 'error_id' in oss:
          if oss['error_id'] == 'NOAUTH':
              reauth()
              continue
          else:
              raise '%s: %s' % (oss['error_id'], oss['error'])

      oss = oss['response'][entry.object_field]

      if not oss:
          break

      min_id = max(i['id'] for i in oss) + 1

      results = (transform(i, entry.columns) for i in oss)
      for result in results:
          outfile.write('\t'.join(result) + '\n')
