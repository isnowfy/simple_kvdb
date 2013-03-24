# -*- coding: utf-8 -*-

import json
import time

class NotSupportedException(Exception):
    pass

class SkvdbBase(object):
    def get(self, key):
        raise NotImplementedError()

    def set(self, key, value, timeout=None):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    def exists(self, key):
        raise NotImplementedError()

class RedisSkvdb(SkvdbBase):
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        import redis
        self.db = redis.Redis(host, port, db, password)

    def set(self, key, value, timeout=None):
        self.db.set(key, json.dumps(value))
        if timeout:
            self.db.expire(key, timeout)

    def get(self, key):
        value = self.db.get(key)
        if value is None:
            return None
        return json.loads(value)

    def delete(self, key):
        self.db.delete(key)

    def exists(self, key):
        return self.db.exists(key)

class MongodbSkvdb(SkvdbBase):
    def __init__(self, host='localhost', port=27017, db='skvdb',
                 user=None, password=None):
        import pymongo
        mongo_con = pymongo.Connection(host, port)
        self.db = mongo_con[db]
        if user and password:
            self.db.authenicate(user, password)

    def set(self, key, value, timeout=None):
        if timeout:
            timeout += time.time()
        data = {'key': key, 'value': value, 'timeout': timeout}
        if self.exists(key):
            self.db.skvdb.update({'key': key}, data)
        else:
            self.db.skvdb.insert(data)

    def get(self, key):
        if self.exists(key):
            return self.db.skvdb.find_one({'key': key})['value']
        return None

    def delete(self, key):
        self.db.skvdb.remove({'key': key})

    def exists(self, key):
        now = time.time()
        data = self.db.skvdb.find_one({'key': key})
        if not data:
            return False
        if data['timeout'] and data['timeout'] < now:
            self.delete(key)
            return False
        return True

class SaeSkvdb(SkvdbBase):
    def __init__(self):
        import sae.kvdb
        self.db = sae.kvdb.KVClient()

    def set(self, key, value, timeout):
        self.db.set(key, json.dumps(value), timeout)

    def get(self, key):
        value = self.db.get(key)
        if value is None:
            return None
        return json.loads(self.db.get(key))

    def delete(self, key):
        kv.delete(key)

    def exists(self, key):
        return self.get(key) is not None

class GaeSkvdb(SkvdbBase):
    def __init__(self):
        from google.appengine.ext import db as gdb
        class GSkvdb(gdb.Model):
            key = gdb.StringProperty()
            value = gdb.StringProperty()
            timeout = gdb.FloatProperty()
        self.model = GSkvdb
        self.db = gdb

    def set(self, key, value, timeout):
        if timeout:
            timeout += time.time()
        value = json.dumps(value)
        data = self.model(key=key, value=value, timeout=timeout)
        data.put()

    def get(self, key):
        if self.exists(key):
            tmp = self.db.GqlQuery('SELECT * FROM GSkvdb WHERE key=:1', key)
            data = tmp.get()
            return json.loads(data.value)
        return None

    def delete(self, key):
        tmp = self.db.GqlQuery('SELECT * FROM GSkvdb WHERE key=:1', key)
        data = tmp.get()
        if data:
            data.delete()

    def exists(self):
        now = time.time()
        tmp = self.db.GqlQuery('SELECT * FROM GSkvdb WHERE key=:1', key)
        data = tmp.get()
        if not data:
            return False
        if data.timeout and datatimeout < now:
            self.db.skvdb.remove({'key': key})
            return False
        return True


call = {'redis': RedisSkvdb, 'mongodb': MongodbSkvdb, 'sae': SaeSkvdb,
        'gae': GaeSkvdb}

def get_skvdb(engine, *args, **argkw):
    if engine in call:
        return call[engine](*args, **argkw)
    else:
        raise NotSupportedException()
