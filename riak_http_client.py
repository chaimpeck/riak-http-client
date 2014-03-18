import sys
import random
import urllib
from cStringIO import StringIO
import mimetools
from rfc822 import parsedate_tz, mktime_tz

import simplejson as json
import pycurl

class HttpResponse:
    def __init__(self, body, headers, code, message):
        self.body = body
        self.headers = headers
        self.code = code
        self.message = message
        
class RiakException(Exception):
    pass
    
class RiakBadRequestException(RiakException):
    pass
    
class RiakNoContentException(RiakException):
    pass

class RiakUnknownException(RiakException):
    pass

class RiakHttpClient:
    """An client to access Riak via http and perform basic operations"""
    
    def __init__(self, nodes, bucket, https=False, port=8098):
        """Initialize RiakHttpClient
        nodes: A list of strings representing the Riak nodes
        bucket: The name of the bucket (a string)
        """
        
        self.nodes = nodes
        self.bucket = bucket
        self.https = https
        self.port = port
        
    def _get_base_url(self):
        node = random.choice(self.nodes)
        
        if self.https:
            protocol = "https://"
        else:
            protocol = "http://"
        
        base_url = protocol+node.strip('/')+":"+str(self.port)
        base_url += "/buckets/"+self.bucket.strip('/')
        
        return base_url
        
    def _make_request(self, loc, method="GET", data=None, headers={}):
        base_url = self._get_base_url()
        
        url = base_url + "/" + loc
        
        body_buf = StringIO()
        headers_buf = StringIO()
        
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEFUNCTION, body_buf.write)
        c.setopt(c.HEADERFUNCTION, headers_buf.write)
        c.setopt(c.CUSTOMREQUEST, method)
        
        if data:
            c.setopt(c.POSTFIELDS, data)
        
        c.setopt(pycurl.HTTPHEADER, ["%s: %s" % i for i in headers.items()])
        
        c.perform()
        
        body = body_buf.getvalue()
        response_headers = headers_buf.getvalue()
        
        body_buf.close()
        headers_buf.close()
        
        # Pop the first line for further processing
        response_line, response_headers = response_headers.split('\r\n', 1)   
        
        resp, code, message = response_line.split(' ', 2)

        # Get the headers
        m = mimetools.Message(StringIO(response_headers))
        
        response = HttpResponse(body=body, headers=m, code=code, message=message)
        
        self._raise_for_errors(response)
        
        return response
    
    
    def _raise_for_errors(self, response):
        if response.code in ["200", "201", "204"]:
            return
        
        error_message = "HTTP Code %s: %s" % (response.code, response.message)
        
        if response.code == "404":
            raise RiakNoContentException(error_message)
        elif response.code == '400':
            raise RiakBadRequestException(error_message)
        else:
            raise RiakUnknownException(error_message)
            
    
    def get(self, key):
        """Get an item with key"""
    
        base_url = self._get_base_url()
        
        url = base_url + "/keys/" + key
        
        response = self._make_request("keys/" + key)
        
        print response.headers
        
        if response.headers.get('Content-Type') == "application/json":
            return json.load(response.body)
        else:
            return response.body
        
        return output
            
    def put(self, key, data, content_type=None, content_encoding=None,
            meta_headers={}, indexes={}):
        """Put the item at key."""
        
        base_url = self._get_base_url()
        
        headers = {}
        
        print content_type
        
        if content_type:
            headers['Content-Type'] = content_type
        else:
            headers['Content-Type'] = 'application/octet-stream'
            
        if content_encoding:
            headers['Content-Encoding'] = content_encoding
        
        for header_name, value in meta_headers.items():
            headers['X-Riak-Meta-'+header_name] = value
        
        for index_name, value in indexes.items():
            headers['X-Riak-Index-'+index_name] = value
        
        print headers
        
        self._make_request("keys/" + key + "?returnbody=true", method="PUT", 
                           data=data, headers=headers)
        
    
    def delete(self, key):
        """Delete the item at key."""
        
        self._make_request("keys/" + key, method="DELETE")
        
    
    def get_bucket_properties(self):
        response = self._make_request("props")
        
        return json.loads(response.body)
        

def main(args):
    nodes = ["dp%s.prod6.ec2.cmg.net" % (i + 1) for i in range(5)]
    bucket = "wcc_dev"
    client = RiakHttpClient(nodes, bucket)
    
    client.put("blah", "4test", meta_headers={'SomeHeader':2}, indexes={'domain_bin':'blah'})
    print client.get("blah")
    client.delete("blah")
    
    #print client.get_bucket_properties()
    
    
# command line testing
if __name__ == "__main__":
    main(sys.argv[1:])