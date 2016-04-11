from redis.sentinel import Sentinel
from pymongo import MongoClient
'''
@author: Administrator
'''

class Configuration:
		
	def set_cache_info(self,cache_info):
		self.cache_info = cache_info
		print 'eeee',self.cache_info
		self.sentinel_address=self.cache_info.split('_')[0].split(';')
		self.cache_address=self.cache_info.split('_')[1].split(';')
		self.seninel_ip=self.sentinel_address[0].split(':')[0]
		self.seninel_port=self.sentinel_address[0].split(':')[1]
		self.sentinel=Sentinel([(self.seninel_ip,self.seninel_port)])
		self.sentinel_db1=Sentinel([(self.seninel_ip,self.seninel_port)],db=1)
		#persistent_info = '10.4.200.114:26379;10.4.200.115:26379;10.4.200.116:26379;10.4.200.117:26379;10.4.200.118:26379_10.4.200.119:6379;10.4.200.121:6379;10.4.200.123:6379;10.4.200.125:6379'
		#self.set_persistent_info(persistent_info)

	def set_persistent_info(self,persistent_info):
		self.persistent_info = persistent_info
		#print 'tttt' + persistent_info
		self.replcaset_info = self.persistent_info.split(';')
		self.mongo_client = []
		for replcaset_unit in self.replcaset_info:
			self.mongo_client.append(MongoClient(replcaset_unit.split('_')[0], replicaset = 'PSU_' + replcaset_unit.split('_')[0]))
		#print self.mongo_client
		
