# -*- coding: utf-8 -*-
import urllib2
import httplib
import json
import csv
import inspect
import logging

CALL_URI = 'http://www.icane.es/metadata/api/regional-data/time-series-list'   
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def getLiveJson(url):
    request = urllib2.Request(url, headers={"Accept" : "application/json"})
    return urllib2.urlopen(request)

def findLink(node, links):
    if (str(node["nodeType"]["uriTag"]) == "time-series" or
        str(node["nodeType"]["uriTag"])=="non-olap-native"):
        if links:        
            links.append(node["uri"])
        else:
            links = [node["uri"]]
        return links
    else:
        for child in node["children"]: #child is a dictionary        
            findLink(child, links)
            
    return None
       
def findUris(data):
    if (str(data['nodeType']['uriTag'])=='time-series' or
       str(data['nodeType']['uriTag'])=='non-olap-native'):
        if 'uri' in data and 'apiUris' in data:
            yield data['uri'], data['apiUris'][3]['uri']
    for k in data:
        if isinstance(data[k], list) and k == 'children': #not in ['apiUris','links','sources', 'measures']:
            for i in data[k]:
                for j in findUris(i):
                    yield j
    
def getUris(rootUri):
    #uris = []
    #jsonUris= []
    tuples = []
    content = getLiveJson(rootUri)
    nodes = json.load(content)
    for node in nodes:
        tuples = tuples + list(findUris(node))        
        #for uri, jsonUri in findUris(node):
         #   uris.append(uri)
          #  jsonUris.append(jsonUri)
    return tuples

def main():
    
    tuples = getUris(CALL_URI) #all urls
    logger.info('URLs to analyze:' + str(len(tuples)))
    csv_error_url = open('url_errors.csv', 'wb')
    tuples_ok = []
    try:
        csvwriter = csv.writer(csv_error_url)
        for tuple in tuples:
            request = urllib2.Request(tuple[0],
                                          headers={"Accept": "application/json"})
            try:
                f = urllib2.urlopen(request)
                
            except urllib2.HTTPError, e: 
                logger.error((inspect.stack()[0][3]) +
                                 ': HTTPError = ' + str(e.code) +
                                 ' ' + str(e.reason) +
                                 ' ' + str(e.geturl()))
                csvwriter.writerow(('HTTPError = ' + str(e.code),
                                    str(e.reason), str(e.geturl())))
            except urllib2.URLError, e:
                logger.error('URLError = ' + str(e.reason) +
                                 ' ' + str(e.geturl()))
                csvwriter.writerow(('URLError = ' + str(e.reason),
                                    '', str(e.geturl())))
            except httplib.HTTPException, e:
                logger.error('HTTPException')
                raise
            except Exception:
                import traceback
                logger.error('Generic exception: ' + traceback.format_exc())
                raise
            else:
                tuples_ok.append(tuple)
                f.close()
    finally:
        logger.info('Closing URL error file...')
        csv_error_url.close()
    logger.info('JSON exports to analyze:' + str(len(tuples_ok)))
    csv_error_export = open('export_errors.csv', 'wb')
    try:
        csvwriter = csv.writer(csv_error_export)
        for tuple in tuples_ok:
            request = urllib2.Request(tuple[1],
                                          headers={"Accept": "application/json"})
            try:
                f = urllib2.urlopen(request)
                
            except urllib2.HTTPError, e: 
                logger.error((inspect.stack()[0][3]) +
                                 ': HTTPError = ' + str(e.code) +
                                 ' ' + str(e.reason) +
                                 ' ' + str(e.geturl()))
                csvwriter.writerow(('HTTPError = ' + str(e.code),
                                    str(e.reason), str(e.geturl())))
            except urllib2.URLError, e:
                logger.error('URLError = ' + str(e.reason) +
                                 ' ' + str(e.geturl()))
                csvwriter.writerow(('URLError = ' + str(e.reason),
                                    '', str(e.geturl())))
            except httplib.HTTPException, e:
                logger.error('HTTPException')
                raise
            except Exception:
                import traceback
                logger.error('Generic exception: ' + traceback.format_exc())
                raise
            else:
                try:
                    response = json.loads(f.read())
                except ValueError:
                    logger.error('ValueError: No JSON object could be decoded '
                                 + tuple[1])
                    csvwriter.writerow(('ValueError: No JSON object could be decoded',
                                 ' ' + tuple[1])) 
                size = len(json.dumps(response))
                logger.info('JSON export size: ' + str(size))
                if not (size > 100):
                    csvwriter.writerow(('Size Error',
                                        size, tuple[1]))
                f.close()
    finally:
        logger.info('Closing export error file...')
        csv_error_export.close()
        
if __name__ == '__main__':
    main()