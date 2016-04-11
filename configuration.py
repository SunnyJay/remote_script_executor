"""
    Configuration
    -------
    A class which loads the config info from db and creates the instances of handling the cache & persistent nodes.
    Author: Sun Nanjun<sun_coke007@163.com>
    Created on 2016-03-22
"""

from redis.sentinel import Sentinel
from pymongo import MongoClient

class Configuration:
		
	def set_cache_info(self,cache_info):
		self.cache_info = cache_info
		self.sentinel_address = self.cache_info.split('_')[0].split(';')
		self.cache_address = self.cache_info.split('_')[1].split(';')
		self.seninel_ip = self.sentinel_address[0].split(':')[0]
		self.seninel_port = self.sentinel_address[0].split(':')[1]
		self.sentinel = Sentinel([(self.seninel_ip,self.seninel_port)])
		self.sentinel_db1 = Sentinel([(self.seninel_ip,self.seninel_port)], db = 1)

	def set_persistent_info(self,persistent_info):
		self.persistent_info = persistent_info
		self.replcaset_info = self.persistent_info.split(';')
		self.mongo_client = []

		for replcaset_unit in self.replcaset_info:
			self.mongo_client.append(MongoClient(replcaset_unit.split('_')[0], replicaset = 'PSU_' + replcaset_unit.split('_')[0]))
		
