from logging import getLogger
import re

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


def make_datajson_entry(package, plugin):
    # keywords
    keywords = [t["display_name"] for t in package["tags"]]
    if len(keywords) == 0 and plugin.default_keywords is not None:
        keywords = re.split("\s*,\s*", plugin.default_keywords)

    # form the return value as an ordered list of fields which is nice for doing diffs of output
    ret = [
        ("title", package["title"]),
        ("description", package["notes"]),
        ("keyword", keywords),
        #HHS:
        #("modified", extra(package, "Date Updated", datatype="iso8601", default=extra(package, "Date Released", datatype="iso8601"))),
        ("modified", extra(package, "Metadata Date")),
        #("publisher", package["author"]),
        #("publisher", json.loads(extra(package, "responsible-party")[0]).get("name")),
        #("publisher", extra(package, "responsible-party")),
        #("publisher", type(extra(package, "responsible-party"))),
        #("publisher", json.loads(extra(package, "responsible-party").replace("\\", "").replace("\[", "").replace("\]", "")).get("name")),
        #("publisher", extra(package, "responsible-party").replace("\\", "").replace("\[", "").replace("\]", "")),
        #HHS:
        #("publisher", package["author"]),
        ("publisher", get_responsible_party(extra(package, "Responsible Party"))),
        #HHS:
        #("bureauCode", extra(package, "Bureau Code").split(" ") if extra(package, "Bureau Code") else None),
        ("bureauCode", bureau_code(package)),
        ("programCode", extra(package, "Program Code").split(" ") if extra(package, "Program Code") else None),
        #("contactPoint", extra(package, "Contact Name")),
        #("contactPoint", json.loads(extra(package, "responsible-party")[0]).get("name")),
        #("contactPoint", extra(package, "responsible-party")),
        #("contactPoint", type(extra(package, "responsible-party"))),
        #("contactPoint", json.loads(extra(package, "responsible-party").replace("\\", "").replace("\[", "").replace("\]", "")).get("name")),
        #("contactPoint", extra(package, "responsible-party").replace("\\", "").replace("\[", "").replace("\]", "")),
        ("contactPoint", contact_point(package, default=plugin.default_contactpoint)),
        
        ("mbox", extra(package, "Contact Email", default=plugin.default_mbox)),
        ("identifier", package["id"]),
        ("accessLevel", extra(package, "Access Level", default="public")),
        ("accessLevelComment", extra(package, "Access Level Comment")),
        ("dataDictionary", extra(package, "Data Dictionary")),
        ("accessURL", get_primary_resource(package).get("url", None)),
        ("webService", get_api_resource(package).get("url", None)),
        ("format", extension_to_mime_type(get_primary_resource(package).get("format", None)) ),
        ("license", extra(package, "Licence")),
        ("spatial", extra(package, "Spatial")),
        ("temporal", build_temporal(package)),
        #HHS:
        #("issued", extra(package, "Date Released", datatype="iso8601")),
        ("issued", get_reference_date(extra(package, "Dataset Reference Date"))),
        #HHS:
        #("accrualPeriodicity", extra(package, "Publish Frequency")),
        ("accrualPeriodicity", extra(package, "Frequency Of Update")),
        ("language", extra(package, "Language")),
        ("PrimaryITInvestmentUII", extra(package, "PrimaryITInvestmentUII")),
        ("dataQuality", extra(package, "Data Quality Met", default="true") == "true"),
        ("theme", [s for s in (extra(package, "Subject Area 1"), extra(package, "Subject Area 2"), extra(package, "Subject Area 3")) if s != None]),
        ("references", [s for s in extra(package, "Technical Documentation", default="").split(" ") if s != ""]),
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
    ]

    # Special case to help validation.
    if extra(package, "Catalog Type") == "State Catalog":
        ret.append( ("_is_federal_dataset", False) )

    # GSA doesn't like null values and empty lists so remove those now.
    ret = [(k, v) for (k, v) in ret if v is not None and (not isinstance(v, list) or len(v) > 0)]

    # And return it as an OrderedDict because we need dict output in JSON
    # and we want to have the output be stable which is helpful for debugging (e.g. with diff).
    return OrderedDict(ret)
    
def extra(package, key, default=None, datatype=None, raise_if_missing=False):
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
        if extra["key"] == key:
            v = extra["value"]

            if datatype == "iso8601":
                # Hack: If this value is a date, convert Drupal style dates to ISO 8601
                # dates by replacing the space date/time separator with a T. Also if it
                # looks like a plain date (midnight time), remove the time component.
                v = v.replace(" ", "T")
                v = v.replace("T00:00:00", "")

            return v
    if raise_if_missing: raise ValueError("Missing value for %s.", key)

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

    # If this came from a harvested data.json file, we marked the resource
    # that came from the top-level accessURL as 'is_primary_distribution'.
    for r in package["resources"]:
        if r.get("is_primary_distribution") == 'true':
            return r

    # Otherwise fall back to a resource by prefering certain formats over others.
    return get_best_resource(package, ("csv", "xls", "xml", "text", "zip", "rdf", "text/html"), ("api", "query tool", "widget"))
    
def get_api_resource(package):
    # Return info about an API resource.
    return get_best_resource(package, ("api",))

def build_temporal(package):
    # Build one dataset entry of the data.json file.
    #HHS:
    #try:
    #    # we ask extra() to raise if either the start or end date is missing since we can't
    #    # form a valid value in that case
    #    return \
    #          extra(package, "Coverage Period Start", datatype="iso8601", raise_if_missing=True) \
    #        + "/" \
    #        + extra(package, "Coverage Period End", datatype="iso8601", raise_if_missing=True)
    #except ValueError:
    #    return None

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
    
