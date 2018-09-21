from flask import Flask, jsonify, request, Response
from flask_pymongo import PyMongo
import json
import time
import re
from bson import ObjectId
import hashlib
from hashlib import md5
from werkzeug.datastructures import Headers
from re import findall
# import sys, os
# print(os.path.dirname(sys.executable))

app = Flask(__name__)
# app.config['MONGO_DBNAME'] = 'witbucket'
# app.config['MONGO_URI'] = 'mongodb://muic:aaaaa11111@ds159782.mlab.com:59782/witbucket'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/witbucket'

mongo = PyMongo(app)

bucket_data = 'buckets'

@app.route('/<bucketname>', methods=['POST','GET','DELETE'])
def bucket(bucketname):
    if( bucket_data not in mongo.db.list_collection_names()):
        mongo.db.create_collection(bucket_data)

    error_response = jsonify({'status': 'ERROR'})
    error_response.status_code = 400
    if(request.args.get("create")=='') and request.method == 'POST': # create
        if(re.match('^[a-zA-Z0-9\-\_]+$', bucketname) and bucketname not in mongo.db.list_collection_names()):
            metadata = {"created": int(time.time()), 
                        "modified": int(time.time()),
                        "name": bucketname}
            ret = jsonify(metadata)
            mongo.db['buckets'].insert_one(metadata)
            mongo.db.create_collection(bucketname)
            return ret
        else:
            return error_response

    elif(request.args.get("delete")=='') and request.method == 'DELETE': # delete
        if(bucketname in mongo.db.list_collection_names() and bucketname is not bucket_data):
            b = mongo.db['buckets'].find_one({'name': bucketname})
            mongo.db['buckets'].remove(b)
            mongo.db[bucketname].drop()
            return jsonify({'status': 'SUCCESS'})
        else:
            return error_response
        

    elif(request.args.get("list")=='') and request.method == 'GET': # list
        clist = mongo.db[bucket_data].find_one({'name':bucketname})
        if(clist):
            clist.pop("_id", None)
            objs = mongo.db[bucketname].find()
            lst = []
            for obj in objs:
                if(obj['completed'] == 1):
                    lst.append({'name':obj['name'],
                            'eTag':obj['etag']})
            clist['objects'] = lst
            return jsonify(clist)
        else:
            return error_response

    return error_response

@app.route('/<bucketname>/<objectname>', methods=['POST'])
def object_post(bucketname, objectname):
    error_response = jsonify({'status': 'ERROR'})
    error_response.status_code = 400
    if(request.args.get("create")=='' and bucketname in mongo.db.list_collection_names() and (re.match('^(?!\.)[a-zA-Z0-9\-\_\.]+(?<!\.)$', objectname))): # Create object
        bcket = mongo.db[bucketname]
        bcket.insert_one({'name':objectname, 'completed': 0, 'parts':{}, 'metadata': {}})

        return jsonify({'status': 'SUCCESS'})
    elif(request.args.get("complete")=='' and bucketname in mongo.db.list_collection_names() and mongo.db[bucketname].find_one({'name':objectname})): # Complete Multi-part upload
        db = mongo.db[bucketname]
        obj = db.find_one({'name':objectname})
        temp = obj['parts'][sorted(obj['parts'])[0]]
        length = len(temp)
        for part in sorted(obj['parts'])[1:]:
            temp+=obj['parts'][part]
            length+=len(obj['parts'][part])
        etag = str(hashlib.sha1(temp).hexdigest())+'-'+str(len(obj['parts']))
        obj['completed'] = 1
        obj['etag'] = etag

        db.save(obj)
        return jsonify({ "eTag": etag, "length": length, "name": obj['name'] })

    return error_response

@app.route('/<bucketname>/<objectname>', methods=['PUT'])
def object_put(bucketname, objectname):
    db = mongo.db[bucketname]
    obj = db.find_one({'name':objectname})
    if bucketname not in mongo.db.list_collection_names():
            ret["error"] = "InvalidBucket"
            error_response = jsonify(ret)
            error_response.status_code = 400
            return error_response 
    if not obj:
        ret["error"] = "InvalidObjectName"
        error_response = jsonify(ret)
        error_response.status_code = 400
        return error_response
    if(request.args.get("partNumber")):  # upload part
        partNumber = int(request.args.get("partNumber"))
        
        partSize, partMd5 = request.headers.get("Content-Length"), request.headers.get("Content-MD5")
        data = request.get_data()
        ret = {"md5": partMd5, "length": partSize, "partNumber": partNumber}

        if not (1 <= partNumber <=10000):
            ret["error"] = "InvalidPartNumber"
            error_response = jsonify(ret)
            error_response.status_code = 400
            return error_response 
        if obj['completed']==1:
            ret["error"] = "InvalidFlag"
            error_response = jsonify(ret)
            error_response.status_code = 400
            return error_response
        if(len(request.data) != int(partSize)):
            ret["error"] = "LengthMismatched"
            error_response = jsonify(ret)
            error_response.status_code = 400
            return error_response
        if(str(hashlib.sha1(request.data).hexdigest()) != str(partMd5)):
            ret["error"] = "MD5Mismatched"
            error_response = jsonify(ret)
            error_response.status_code = 400
            return error_response

        obj['parts'][str(partNumber)]= request.data
        db.save(obj)
        return jsonify(ret)
    elif(request.args.get("metadata")=='' and request.args.get("key")): #Add/update object metadata by key
        value = request.headers.get("value")
        key = request.args.get("key")
        obj['metadata'][key] = value
        db.save(obj)
        return jsonify({'status': 'SUCCESS'})


    error_response = jsonify({'status': 'ERROR'})
    error_response.status_code = 400
    return error_response


@app.route('/<bucketname>/<objectname>', methods=['DELETE'])
def object_delete(bucketname, objectname):
    db = mongo.db[bucketname]
    obj = db.find_one({'name':objectname})

    if bucketname not in mongo.db.list_collection_names():
        ret["error"] = "InvalidBucket"
        error_response = jsonify(ret)
        error_response.status_code = 400
        return error_response 
    if not obj:
        ret["error"] = "InvalidObjectName"
        error_response = jsonify(ret)
        error_response.status_code = 400
        return error_response

    if(request.args.get("partNumber")):
        partNumber = int(request.args.get("partNumber"))
        ret = {"partNumber": partNumber}
        if not (1 <= partNumber <=10000):
            ret["error"] = "InvalidPartNumber"
            error_response = jsonify(ret)
            error_response.status_code = 400
            return error_response 
        
        if obj['completed']==1:
            ret["error"] = "InvalidFlag"
            error_response = jsonify(ret)
            error_response.status_code = 400
            return error_response
        if(str(partNumber) not in obj['parts']):
            et["error"] = "InvalidPartNumber"
            error_response = jsonify(ret)
            error_response.status_code = 400
            return error_response

        obj['parts'].pop(str(partNumber), None)
        db.save(obj)

        return jsonify({'status': 'SUCCESS'})
    elif(request.args.get("delete")=='' and bucketname not in mongo.db.list_collection_names() and mongo.db[bucketname].find_one({'name':objectname})):
        db.remove(obj)
        return jsonify({'status': 'SUCCESS'})
    elif request.args.get("metadata")=='':
        key = request.args.get("key")
        obj['metadata'].pop(key, None)
        db.save(obj)
        return jsonify({'status': 'SUCCESS'})

    error_response = jsonify({'status': 'ERROR'})
    error_response.status_code = 400
    return error_response


@app.route('/<bucketname>/<objectname>', methods=['GET'])
def object_get(bucketname, objectname):

    db = mongo.db[bucketname]
    obj = db.find_one({'name':objectname})

    if bucketname not in mongo.db.list_collection_names():
        ret["error"] = "InvalidBucket"
        error_response = jsonify(ret)
        error_response.status_code = 400
        return error_response 
    if not obj:
        ret["error"] = "InvalidObjectName"
        error_response = jsonify(ret)
        error_response.status_code = 400
        return error_response

    length = len(obj['parts']['1'])
    for part in sorted(obj['parts'])[1:]:
        length+=len(obj['parts'][part])
    headers = Headers()
    headers.add('Content-Disposition', 'attachment', filename=obj['name'])
    headers.add('Content-Transfer-Encoding','binary')
    status = 200
    size   = length
    begin  = 0;
    end    = size-1;

    f= open('objects/'+obj['name'],"w+b")
    for part in sorted(obj['parts']):
        f.write((obj['parts'][part]))
    f.close

    if request.headers.has_key("Range") and rangerequest:
        status = 206
        headers.add('Accept-Ranges','bytes')
        ranges = findall(r"\d+", request.headers["Range"])
        begin  = int( ranges[0] )
        if len(ranges)>1:
            end = int( ranges[1] )
        headers.add('Content-Range','bytes %s-%s/%s' % (str(begin),str(end),str(end-begin)) )
    
    headers.add('Content-Length',str((end-begin)+1))

    response = Response(f, status=status, headers=headers, direct_passthrough=True)
    return response



if __name__ == '__main__':
    app.run(debug=True)






