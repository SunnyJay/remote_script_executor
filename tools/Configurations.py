from redis.sentinel import Sentinel
'''
@author: Administrator
'''

class Configuration:
	cache_info=''
	sentinel_address=''
	cache_address=''
	seninel_ip=''
	seninel_port=''
	sentinel=None
	sentinel_db1=None
	def set_cache_info(self,cache_info):
		self.cache_info = cache_info
		
	def init_config(self):
		self.sentinel_address=cache_info.split('_')[0].split(';')
		self.cache_address=cache_info.split('_')[1].split(';')
		self.seninel_ip=sentinel_address[0].split(':')[0]
		self.seninel_port=sentinel_address[0].split(':')[1]
		self.sentinel=Sentinel([(seninel_ip,seninel_port)])
		self.sentinel_db1=Sentinel([(seninel_ip,seninel_port)],db=1)