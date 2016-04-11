#-*- coding:utf-8 -*-
"""
    DssDataManagerMongo
    -------
    A class which includes various tools for the persistent/mongoDb cluster. 
    And the tools are registered in the database.
    Author: Sun Nanjun<sun_coke007@163.com>
    Created on 2016-03-22
"""
import operator
from bson.json_util import dumps

class DssDataManagerMongo:

    def set_configurations(self,configurations):
        self.configurations = configurations
        self.init_config()

    def init_config(self):
        self.persistent_info = self.configurations.persistent_info
        self.replcaset_info = self.configurations.replcaset_info
        self.mongo_client = self.configurations.mongo_client

    # returns the persistent_info
    def persistent_info_export(self,args):
		return self.persistent_info + '\n'

    # gets the stats of the MongoDb cluster
    def clusterStats(self,args):
		ret = ''
		i = 0

		for persistent_node in self.mongo_client:
			ret += '*******************************************' + self.replcaset_info[i].split('_')[0] + '*******************************************' + '\n'
			boson_ret = persistent_node.admin.command('replSetGetStatus')
			boson_str = str(dumps(boson_ret, indent=4))
			ret += boson_str + '\n'
			i += 1

		return ret

    # gets the config of the MongoDb cluster
    def clusterConfig(self,args):
		ret = ''
		i = 0

		for persistent_node in self.mongo_client:
			ret += '*******************************************' + self.replcaset_info[i].split('_')[0] + '*******************************************' + '\n'
			boson_ret = persistent_node.admin.command('replSetGetConfig')
			boson_str = str(dumps(boson_ret, indent=4))
			ret += boson_str + '\n'
			i += 1

		return ret

	#finds the msgid
    def findWhereIsMstpId(self,args):
    	ret = ''
        location_client = None
        location_db = None

        for persistent_node in self.mongo_client:
        	db_name_list = persistent_node.database_names()
        	for db_name in db_name_list:
        		if 't_MSGID_SEQID_' in db_name:
        			cursor = persistent_node[db_name].col_msgid_seqid.find({'mstp_id':str(args)})
        			if cursor.count() != 0:
        				location_address = persistent_node.address
        				ret += 'location_address:' + str(location_address) + '\n'
        				ret += 'db_name:' + str(db_name) + '\n'
        				ret += 'item num:' + str(cursor.count()) + '\n\n'
        				ret += 'item detail:\n'
        				for find_result in cursor:
        					ret += str(find_result) + '\n'
        					#ret += str(dumps(find_result,indent=4)) + '\n'
        				return ret

        return 'nothing found...'	

    # finds the latest seqid of one mstpid.
    def findSeqIdOfMstpId(self,args):
    	ret = ''

        for persistent_node in self.mongo_client:
        	db = persistent_node.t_SEQID
        	cursor = db.col_seqid.find({'mstp_id_withPRIO':str(args)})
        	if cursor.count() != 0:
        		location_address = persistent_node.address
        		ret += 'location_address:' + str(location_address) + '\n'
        		ret += 'db_name:' + 't_SEQID' + '\n'
        		ret += 'the lastest seq_id:\n'
        		for find_result in cursor:
        			#ret += str(find_result) + '\n'
        			ret += str(dumps(find_result,indent=4)) + '\n'  
        		return ret

        return 'nothing found...'

    # finds the latest ackseqid of one mstpid.
    def findAckSeqIdOfMstpId(self,args):
       	ret = ''

        for persistent_node in self.mongo_client:	
        	db = persistent_node.t_ACK_SEQID
        	cursor = db.col_ack_seqid.find({'mstp_id_withPRIO':str(args)})
        	if cursor.count() != 0:
        		location_address = persistent_node.address
        		ret += 'location_address:' + str(location_address) + '\n'
        		ret += 'db_name:' + 't_ACK_SEQID' + '\n'
        		ret += 'the lastest ack_seq_id:\n'
        		for find_result in cursor:
        			#ret += str(find_result) + '\n'
        			ret += str(dumps(find_result,indent=4)) + '\n'
        		return ret

        return 'nothing found...' 

    #finds the mstpid
    def findWhereIsMsgId(self,args):
    	ret = ''
        location_client = None
        location_db = None

        for persistent_node in self.mongo_client:
        	db_name_list = persistent_node.database_names()
        	for db_name in db_name_list:
        		if 't_MESSAGE_' in db_name:
        			cursor = persistent_node[db_name].col_msg.find({'msg_id':str(args)})
        			if cursor.count() != 0:
        				location_address = persistent_node.address
        				ret += 'location_address:' + str(location_address) + '\n'
        				ret += 'db_name:' + str(db_name) + '\n'
        				#ret += 'item num:' + str(cursor.count()) + '\n\n'
        				ret += 'msg_content:\n'
        				for find_result in cursor:
        					#ret += str(find_result) + '\n'
        					ret += str(dumps(find_result,indent=4)) + '\n'
        				return ret

        return 'nothing found...'

    # finds the database stats of the specified node.
    def findDbStatus(self,args):
    	ret = ''
    	node_id = args.split(' ')[0]
    	db_name_arg = args.split(' ')[1]
    	index = 0

    	for info in self.replcaset_info:
    		if node_id in info:
    			break
    		index += 1

    	persistent_node = self.mongo_client[index]
    	db_name_list = persistent_node.database_names()

    	for db_name in db_name_list:
        	if db_name_arg in db_name:
        		find_result =  persistent_node[db_name_arg].command('dbStats', 1)
        		ret += str(dumps(find_result,indent=4)) + '\n'
        		return ret

    	return 'nothing found...'

    # finds the indexes of the specified database of the specified node.
    def findDbIndexes(self,args):
    	ret = ''
    	node_id = args.split(' ')[0]
    	db_name_arg = args.split(' ')[1]
    	col_name_arg = args.split(' ')[2]
    	index = 0

    	for info in self.replcaset_info:
    		if node_id in info:
    			break
    		index += 1

    	persistent_node = self.mongo_client[index]
    	db_name_list = persistent_node.database_names()

    	for db_name in db_name_list:
        	if db_name_arg in db_name:
        		find_result =  persistent_node[db_name_arg].get_collection(col_name_arg).list_indexes()
        		ret += str(dumps(find_result, indent=4)) + '\n'
        		return ret

    	return 'nothing found...'

    # finds all the databases of one node.
    def findDb(self,args):
        ret = ''
        node_id = args.split(' ')[0]
        index = 0

        for info in self.replcaset_info:
            if node_id in info:
                break
            index += 1

        persistent_node = self.mongo_client[index]
        db_name_list = persistent_node.database_names()
        ret += 'db num:' + str(len(db_name_list)) + '\n\n'

        for db_name in db_name_list:
            ret += db_name + '\n'

        return ret

    # flushes the db
    def flushDb(self,args):
        ret = ''
        confirm_str = args.split(' ')[0]

        if confirm_str != 'OK':
            return 'did nothing'
        try:
            for persistent_node in self.mongo_client:
                db_name_list = persistent_node.database_names()
                for db_name in db_name_list:
                    if 't_MSGID_SEQID_' in db_name or 't_MESSAGE_' in db_name or 't_SEQID' in db_name or 't_ACK_SEQID' in db_name:
                        print 'flush....'
                    #persistent_node.drop_database(db_name)
        except Exception:
            raise

        return 'flush db success!'