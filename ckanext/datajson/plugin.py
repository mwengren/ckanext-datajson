import ckan.plugins as p

from ckan.lib.base import BaseController, render, c
from pylons import request, response
import json, re

try:
    from collections import OrderedDict # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

import ckan.model

from build_datajson import make_datajson_entry, get_facet_fields
from build_datajsonld import dataset_to_jsonld

class DataJsonPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurer)
    p.implements(p.interfaces.IRoutes, inherit=True)
    p.implements(p.interfaces.IFacets)

    # IConfigurer
    
    def update_config(self, config):
    	# Must use IConfigurer rather than IConfigurable because only IConfigurer
    	# is called before after_map, in which we need the configuration directives
    	# to know how to set the paths.

        # TODO commenting out enterprise data inventory for right now
        # DataJsonPlugin.route_edata_path = config.get("ckanext.enterprisedatajson.path", "/enterprisedata.json")
        DataJsonPlugin.route_enabled = config.get("ckanext.datajson.url_enabled", "True")=='True'
        DataJsonPlugin.route_path = config.get("ckanext.datajson.path", "/data.json")
        DataJsonPlugin.route_ld_path = config.get("ckanext.datajsonld.path", re.sub(r"\.json$", ".jsonld", DataJsonPlugin.route_path))
        DataJsonPlugin.ld_id = config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))
        DataJsonPlugin.ld_title = config.get("ckan.site_title", "Catalog")
        DataJsonPlugin.site_url = config.get("ckan.site_url")
        DataJsonPlugin.default_contactpoint = config.get("ckanext.datajson.default_contactpoint")
        DataJsonPlugin.default_mbox = config.get("ckanext.datajson.default_mbox")

        # Adds our local templates directory. It's smart. It knows it's
        # relative to the path of *this* file. Wow.
        p.toolkit.add_template_directory(config, "templates")

    # IRoutes

    def before_map(self, m):
        return m
    
    def after_map(self, m):

        if DataJsonPlugin.route_enabled:
            # /data.json and /data.jsonld (or other path as configured by user)
            m.connect('datajson', DataJsonPlugin.route_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_json')
            # TODO commenting out enterprise data inventory for right now
            #m.connect('enterprisedatajson', DataJsonPlugin.route_edata_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_enterprise')
            m.connect('datajsonld', DataJsonPlugin.route_ld_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_jsonld')

        # TODO DWC update action
        # /data/{org}/data.json
        m.connect('public_data_listing', '/organization/{org}/data.json', controller='ckanext.datajson.plugin:DataJsonController', action='generate_pdl')

        # TODO DWC update action
        # /data/{org}/edi.json
        m.connect('enterprise_data_inventory', '/organization/{org}/edi.json', controller='ckanext.datajson.plugin:DataJsonController', action='generate_edi')

        # /pod/validate
        m.connect('datajsonvalidator', "/pod/validate", controller='ckanext.datajson.plugin:DataJsonController', action='validator')

        # /pod/data-listing
        m.connect('datajsonhtml', "/pod/data-catalog", controller='ckanext.datajson.plugin:DataJsonController', action='show_html_rendition')
        
        return m

    # IFacets
    
    def dataset_facets(self, facets, package_type):
        # Add any facets specified in build_datajson.get_facet_fields() to the top
        # of the facet list, and then put the CKAN default facets below that.
        f = OrderedDict()
        f.update(get_facet_fields())
        f.update(facets)
        return f
    def group_facets(self, facets_dict, group_type, package_type):
        return facets_dict
    def organization_facets(self, facets_dict, organization_type, package_type):
        return facets_dict

class DataJsonController(BaseController):
    def generate_output(self, format):
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'
        
        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]

        #TODO special processing for enterprise
        # output
        data = make_json()
        
        if format == 'json-ld':
            # Convert this to JSON-LD.
            data = OrderedDict([
                ("@context", OrderedDict([
                    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
                    ("dcterms", "http://purl.org/dc/terms/"),
                    ("dcat", "http://www.w3.org/ns/dcat#"),
                    ("foaf", "http://xmlns.com/foaf/0.1/"),
                    ("pod", "http://project-open-data.github.io/schema/2013-09-20_1.0#"),
                    ])
                ),
                ("@id", DataJsonPlugin.ld_id),
                ("@type", "dcat:Catalog"),
                ("dcterms:title", DataJsonPlugin.ld_title),
                ("rdfs:label", DataJsonPlugin.ld_title),
                ("foaf:homepage", DataJsonPlugin.site_url),
                ("dcat:dataset", [dataset_to_jsonld(d) for d in data]),
            ])
            
        return p.toolkit.literal(json.dumps(data, indent=2))

    def generate_json(self):
        return self.generate_output('json')
        
    def generate_jsonld(self):
        return self.generate_output('json-ld')

    ## TODO commenting out enterprise data inventory for right now
    #def generate_enterprise(self):
    #    return self.generate_output('enterprise')
        
    def validator(self):
        # Validates that a URL is a good data.json file.
        if request.method == "POST" and "url" in request.POST and request.POST["url"].strip() != "":
            c.source_url = request.POST["url"]
            c.errors = []
            
            import urllib, json
            from datajsonvalidator import do_validation
            body = None
            try:
                body = json.load(urllib.urlopen(c.source_url))
            except IOError as e:
                c.errors.append(("Error Loading File", ["The address could not be loaded: " + unicode(e)]))
            except ValueError as e:
                c.errors.append(("Invalid JSON", ["The file does not meet basic JSON syntax requirements: " + unicode(e) + ". Try using JSONLint.com."]))
            except Exception as e:
                c.errors.append(("Internal Error", ["Something bad happened while trying to load and parse the file: " + unicode(e)]))
                
            if body:
                try:
                    do_validation(body, c.source_url, c.errors)
                except Exception as e:
                    c.errors.append(("Internal Error", ["Something bad happened: " + unicode(e)]))
                if len(c.errors) == 0:
                    c.errors.append(("No Errors", ["Great job!"]))
            
        return render('datajsonvalidator.html')

    def show_html_rendition(self):
        # Shows an HTML rendition of the data.json file. Requests the file live
        # from http://localhost/data.json.
            
        import urllib, json
        try:
            c.catalog_data = json.load(urllib.urlopen("http://localhost/data.json"))
        except:
            c.catalog_data = []
                
        c.catalog_data.sort(key = lambda x : x.get("modified"), reverse=True)

        return render('html_rendition.html')

def make_json():
    # Build the data.json file.
    packages = p.toolkit.get_action("current_package_list_with_resources")(None, {})
    return [make_datajson_entry(pkg, DataJsonPlugin) for pkg in packages if pkg["type"] == "dataset"]
    

