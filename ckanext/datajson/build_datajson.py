from logging import getLogger
try:
    from collections import OrderedDict # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from  ckan.lib import helpers as h
from  ckanext.spatial.helpers import get_responsible_party, get_reference_date
import os
import json

log = getLogger(__name__)
#log = getLogger("datajson.build_datajson")

def get_facet_fields():
    # Return fields that we'd like to add to default CKAN faceting. This really has
    # nothing to do with exporting data.json but it's probably a common consideration.
    facets = OrderedDict()
    facets["Agency"] = "Publishers" # using "author" produces weird results because the Solr schema indexes it as "text" rather than "string"
    facets["SubjectArea1"] = "Subjects" # search facets remove spaces from field names
    return facets

def make_datajson_entry(package):
    log.info("OrderedDict class name: %s".format(OrderedDict.__name__))
    
    return OrderedDict([
        ("title", package["title"]),
        ("description", package["notes"]),
        #("keyword", [t["display_name"] for t in package["tags"]]),
        #("keyword", [t for t in package["extras"]["tags"].split(",")]),
        ("keyword", tags(package)),
        #("modified", extra(package, "Date Updated")),
        ("modified", extra(package, "Metadata Date")),
        #("publisher", package["author"]),
        #("publisher", json.loads(extra(package, "responsible-party")[0]).get("name")),
        #("publisher", extra(package, "responsible-party")),
        #("publisher", type(extra(package, "responsible-party"))),
        #("publisher", json.loads(extra(package, "responsible-party").replace("\\", "").replace("\[", "").replace("\]", "")).get("name")),
        #("publisher", extra(package, "responsible-party").replace("\\", "").replace("\[", "").replace("\]", "")),
        ("publisher", get_responsible_party(extra(package, "Responsible Party"))),
        
        #("bureauCode", extra(package, "Bureau Code").split(" ") if extra(package, "Bureau Code") else None),
        ("bureauCode", bureau_code(package)),
        ("programCode", extra(package, "Program Code").split(" ") if extra(package, "Program Code") else None),
        #("contactPoint", extra(package, "Contact Name")),
        #("contactPoint", json.loads(extra(package, "responsible-party")[0]).get("name")),
        #("contactPoint", extra(package, "responsible-party")),
        #("contactPoint", type(extra(package, "responsible-party"))),
        #("contactPoint", json.loads(extra(package, "responsible-party").replace("\\", "").replace("\[", "").replace("\]", "")).get("name")),
        #("contactPoint", extra(package, "responsible-party").replace("\\", "").replace("\[", "").replace("\]", "")),
        ("contactPoint", contact_point(package)),
        
        ("mbox", extra(package, "Contact Email")),
        ("identifier", package["id"]),
        ("accessLevel", extra(package, "Access Level", default="public")),
        ("accessLevelComment", extra(package, "Access Level Comment")),
        ("dataDictionary", extra(package, "Data Dictionary")),
        ("accessURL", get_primary_resource(package).get("url", None)),
        ("webService", get_api_resource(package).get("url", None)),
        ("format", [ extension_to_mime_type(get_primary_resource(package).get("format", None)) ]),
        ("license", extra(package, "Licence")),
        ("spatial", extra(package, "Spatial")),
        ("temporal", build_temporal(package)),
        ("issued", get_reference_date(extra(package, "Dataset Reference Date"))),
        ("accrualPeriodicity", extra(package, "Frequency Of Update")),
        ("language", extra(package, "Language")),
        ("PrimaryITInvestmentUII", extra(package, "PrimaryITInvestmentUII")),
        ("granularity", "/".join(x for x in [extra(package, "Unit of Analysis"), extra(package, "Geographic Granularity")] if x != None)),
        ("dataQuality", extra(package, "Data Quality Met", default="true") == "true"),
        ("theme", [s for s in (extra(package, "Subject Area 1"), extra(package, "Subject Area 2"), extra(package, "Subject Area 3")) if s != None]),
        ("references", [s for s in [extra(package, "Technical Documentation")] if s != None]),
        ("landingPage", package["url"]),
        ("systemOfRecords", extra(package, "System Of Records")),
        ("distribution",
            [
                OrderedDict([
                   ("identifier", r["id"]), # NOT in POD standard, but useful for conversion to JSON-LD
                   ("accessURL", r["url"]),
                   ("format", extension_to_mime_type(r["format"])),
                ])
                for r in package["resources"]
                if r["format"].lower() not in ("api", "query tool", "widget")
            ]),
    ])
    
def extra(package, key, default=None):
    # Retrieves the value of an extras field.
    '''
    for extra in package["extras"]:
        if extra["key"] == "extras_rollup":
            extras_rollup_dict = extra["value"]
            #return(extras_rollup_dict) #returns full json-formatted 'value' field of extras_rollup
            extras_rollup_dict = json.loads(extra["value"])
            for rollup_key in extras_rollup_dict.keys():
                if rollup_key == key: return extras_rollup_dict.get(rollup_key)
        
    return default
    '''
    
    current_extras = package["extras"]
    #new_extras =[]
    new_extras = {}
    for extra in current_extras:
        if extra['key'] == 'extras_rollup':
            rolledup_extras = json.loads(extra['value'])
            for k, value in rolledup_extras.iteritems():
                log.info("rolledup_extras key: %s, value: %s", k, value)
                #new_extras.append({"key": k, "value": value})
                new_extras[k] = value
        #else:
        #    new_extras.append(extra)
    
    #decode keys:
    for k, v in new_extras.iteritems():
        k = k.replace('_', ' ').replace('-', ' ').title()
        if isinstance(v, (list, tuple)):
            v = ", ".join(map(unicode, v))
        log.info("decoded values key: %s, value: %s", k, v)
        if k == key:
            return v
    return default

def tags(package, default=None):
    # Retrieves the value of an extras field.
    for extra in package["extras"]:
        if extra["key"] == "tags":
            keywords = extra["value"].split(",")
            return keywords

def bureau_code(package, default=None):
    file = open(os.path.join(os.path.dirname(__file__),"resources") + "/omb-agency-bureau-treasury-codes.json", 'r');
    codelist = json.load(file)
    for bureau in codelist:
        if bureau['Agency'] == package["organization"]["title"]: return "[{0}:{1}]".format(bureau["OMB Agency Code"], bureau["OMB Bureau Code"])
    return default

def contact_point(package, default=None):
    if extra(package, "Contact Name") is not None: return extra(package, "Contact Name")
    elif get_responsible_party(extra(package, "Responsible Party")) is not None: return get_responsible_party(extra(package, "Responsible Party"))
    else: return default


def get_best_resource(package, acceptable_formats, unacceptable_formats=None):
    resources = list(r for r in package["resources"] if r["format"].lower() in acceptable_formats)
    if len(resources) == 0:
        if unacceptable_formats:
            # try at least any resource that's not unacceptable
            resources = list(r for r in package["resources"] if r["format"].lower() not in unacceptable_formats)
        if len(resources) == 0:
            # there is no acceptable resource to show
            return { }
    else:
        resources.sort(key = lambda r : acceptable_formats.index(r["format"].lower()))
    return resources[0]

def get_primary_resource(package):
    # Return info about a "primary" resource. Select a good one.
    return get_best_resource(package, ("csv", "xls", "xml", "text", "zip", "rdf", "text/html"), ("api", "query tool", "widget"))
    
def get_api_resource(package):
    # Return info about an API resource.
    return get_best_resource(package, ("api", "query tool"))

def build_temporal(package):
    # Build one dataset entry of the data.json file.
    temporal = ""
    if extra(package, "Temporal Extent Begin"):
        temporal = extra(package, "Temporal Extent Begin").replace(" ", "T").replace("T00:00:00", "")
    else:
        temporal = extra(package, "Coverage Period Start", "Unknown").replace(" ", "T").replace("T00:00:00", "")
    temporal += "/"
    if extra(package, "Temporal Extent End"):
        temporal += extra(package, "Temporal Extent End").replace(" ", "T").replace("T00:00:00", "")
    else:
        temporal += extra(package, "Coverage Period End", "Unknown").replace(" ", "T").replace("T00:00:00", "")
    if temporal == "Unknown/Unknown": return None
    return temporal

def extension_to_mime_type(file_ext):
    if file_ext is None: return None
    ext = {
        "csv": "text/csv",
        "xls": "application/vnd.ms-excel",
        "xml": "application/xml",
        "rdf": "application/rdf+xml",
        "json": "application/json",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "text": "text/plain",
        "feed": "application/rss+xml",
        "arcgis_rest": "text/html",
        "wms": "text/html",
        "html": "text/html",
        "application/pdf": "application/pdf",
    }
    return ext.get(file_ext.lower(), "application/unknown")
    
