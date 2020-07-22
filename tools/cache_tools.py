# -*- coding: utf-8 -*-
"""
    CacheTools
    -------
    A class which includes various tools for the cache/redis cluster. 
    And the tools are registered in the database.
    Author: Sun Nanjun<sun_coke007@163.com>
    Created on 2016-03-22
"""
import operator
import configuration
import time

class CacheTools:

    def set_configurations(self,configurations):
        self.configurations = configurations
        self.init_config()

    def init_config(self):
        self.cache_info=self.configurations.cache_info
        self.sentinel_address=self.configurations.sentinel_address
        self.cache_address=self.configurations.cache_address
        self.seninel_ip=self.configurations.seninel_ip
        self.seninel_port=self.configurations.seninel_port
        self.sentinel=self.configurations.sentinel
        self.sentinel_db1=self.configurations.sentinel_db1

    # fetches the total number of mstpid in the cache cluster.
    def fetch_all_mstp_id_num(self,args):
        all_num_mstpid = 0
        for cache_node in self.cache_address:
            main = self.sentinel_db1.main_for(cache_node)
            mstpid_list = main.keys('*')
            all_num_mstpid += len(mstpid_list)

        return str(all_num_mstpid)

    # fetches the mstpid whose seqid's number is in the specified range. 
    def fetch_mstp_id_whose_seq_id_is_in_range(self,args):
    	begin_num = int(args.split(' ')[0])
    	end_num = int(args.split(' ')[1])
        ret = ''
        if end_num <= begin_num:
            ret += 'Error Input!'
            return ret

        count = 0
        for cache_node in self.cache_address:
            main = self.sentinel_db1.main_for(cache_node)
            mstpid_list = main.keys('*')
            
            pipe = main.pipeline()
            for mstpid in mstpid_list:
                hlen = pipe.hlen(mstpid)
            lenlist = pipe.execute()

            i = 0
            while i < len(lenlist):
                if begin_num < lenlist[i] < end_num:
                    count += 1
                    ret += mstpid_list[i] + ' ' + str(lenlist[i]) + '\n'
                i += 1

        ret += 'Rate is %.2f%%' % (count*1.0/int(self.fetch_all_mstp_id_num(args))*100)
        return ret
        

    # Flushes the db
    def flush_db(self,args):
        ret = ''
        confirm_str = args.split(' ')[0]
        if confirm_str != 'OK':
            return 'did nothing'

        for cache_node in self.cache_address:
            main0 = self.sentinel.main_for(cache_node)
            main1= self.sentinel_db1.main_for(cache_node)
            #main0.flushDB()
            #main1.flushDB()
            
        for cache_node in self.cache_address:
            main0 = self.sentinel.main_for(cache_node)
            main1= self.sentinel_db1.main_for(cache_node)
            ret += cache_node + '\tdb0:' + str(main0.dbsize()) +'\tdb1:' + str(main1.dbsize()) + '\n'

        return 'flush db success!\n' + ret

    # fetches the ip of the clients which is writing.
    def fetch_writing_client_ip(self,args):
        ret = ''
        for cache_node in self.cache_address:
        	main = self.sentinel.main_for(cache_node)
        	client_list = main.client_list()
        	ip_set = set()
        	for client in client_list:
        		cmd = client['cmd']
        		idle = client['idle']
        		if(cmd == 'set' or cmd == 'hset') and idle < 2:
        			ip_set.add(client['addr'].split(':')[0])
        			ret += ip_set + '\n'    
        	ret += '*******************************************' + '\n'
        return ret

    # fetches the seqid number of each mstpid in the cache cluster.
    def fetch_seq_num_of_each_mstpid(self,args):
        ret = ''
        for cache_node in self.cache_address:
            main = self.sentinel_db1.main_for(cache_node)
            mstpid_list = main.keys('*')
            pipe = main.pipeline()

            for mstpid in mstpid_list:
            	hlen = pipe.hlen(mstpid)
            lenlist = pipe.execute()

            for i in range(len(mstpid_list)):
                ret += str(mstpid_list[i]) + '\t' + str(lenlist[i]) + '\n' 

        #sorted_dict = sorted(dict.iteritems(), key=operator.itemgetter(1), reverse=True)    #sorted by the number of seqid
        return ret
                
    # Deletes the mstpid whose structure is hashtable rather than ziplist in the cache cluster
    def delete_hash_mstp_id(self):
        hash_mstpid_dict = fetch_hash_mstp_id()
        confirm=raw_input('Are you sure to delete?(yes/no)\n')
        if confirm != 'yes':
            exit()
        for item in hash_mstpid_dict:
            main = sentinel_db1.main_for(hash_mstpid_dict[item])
            main.delete(item)
        print 'deleted num:',len(hash_mstpid_dict)

    # fetches the mstpid whose structure is hashtable rather than ziplist in the cache cluster and count its ratio.
    def fetch_hash_mstp_id(self,args):
        findsets = {}
        for cache_node in self.cache_address:
            main = self.sentinel_db1.main_for(cache_node)
            mstpid_list = main.keys('*')
            pipe = main.pipeline()
            for mstpid in mstpid_list:
                pipe.object('encoding',mstpid)
            typelist = pipe.execute()
            for i in range(len(typelist)):
                if typelist[i] == 'hashtable':
                    findsets.setdefault(mstpid_list[i],cache_node)
        ret = ''
        ret += 'Rate is ' + str(len(findsets)*1.0/int(self.fetch_all_mstp_id_num(args))*100) + '%' + '\n'
        for i in findsets:
            ret +=  i + '\t' + str(findsets[i]) + '\n'
        return ret

    # fetches the contents of one mstpid
    def fetch_content_of_mstp_id(self,args):
        mstpid_input = args
        isExists,location = self.fetch_mstp_id_info(mstpid_input)

        if isExists == False:
            return 'The mstpId is not exist!'

        main = self.sentinel_db1.main_for(location)
        content = main.hgetall(mstpid_input)  # 'hgetall' command returns the unordered results.
        data_type = main.object('encoding',mstpid_input)
        main0 = self.sentinel.main_for(location)
        latest_seqId_0 = main0.get(mstpid_input + 'SEQ_0')
        latest_seqId_1 = main0.get(mstpid_input + 'SEQ_1')
        latest_ackId_0 = main0.hget(mstpid_input + 'ACK',0)
        latest_ackId_1 = main0.hget(mstpid_input +'ACK',1)
        
        ret = ''
        ret += 'location:' + location + '\n'
        ret += 'latest seqId 0:' + str(latest_seqId_0) + '\tlatest seqId 1:' + str(latest_seqId_1) + '\n'
        ret += 'latest ackId 0:' + str(latest_ackId_0) + '\tlatest ackId 1:' + str(latest_ackId_1) + '\n'
        ret += 'data type:' + str(data_type) + '\n'
        ret += 'item num:' + str(len(content)) + '\n'
        ret += 'contents:' + '\n'

        seqid_list = main.hkeys(mstpid_input)
        for item in seqid_list:
            ret += item + '\t' + content[item] + '\n'

        return ret
        
    # fetches the msgid info
    def fetch_msg_id_info(self,args):
    	msgId = args
    	ret = ''
        isExists = False
        for cache_node in self.cache_address:
            main = self.sentinel.main_for(cache_node)
            isExists = main.exists(msgId)
            if isExists == True:
                location = cache_node
                ret += 'msgId location:' + cache_node
                break
        if isExists == False:
        	ret += 'can not find msgId:' + msgId
        return ret
        #return isExists,location

    # fetches the mstpid info
    def fetch_mstp_id_info(self,mstpid):
        isExists = False
        location = ''
        for cache_node in self.cache_address:
            main = self.sentinel_db1.main_for(cache_node)
            isExists = main.exists(mstpid + 'MSTPID')
            if isExists == True:
                location= cache_node
                break
                
        return isExists,location

    # fetches the msgid and msgbody of the specified mstpid & seqid
    def fetch_message_info(self,args):
        mstpid_input = args.split(' ')[0]
    	seqid_input = args.split(' ')[1]
    	ret = ''

        for cache_node in self.cache_address:
            main = self.sentinel_db1.main_for(cache_node)
            msgid = main.hget(mstpid_input,seqid_input)
            if msgid != None:
                ret += 'mstpid location:' + cache_node + '\n'
                break

        if msgid == None:
            ret += 'can not find the msgid you want\n'  
            return ret 

        for cache_node in self.cache_address:
            main = self.sentinel.main_for(cache_node)
            msgbody = main.get(msgid)
            if msgbody != None:
                ret += 'message location:' + cache_node + '\n'
                ret += 'msgid:' + msgid + '\n'
                ret += 'msgbody:\n' + 'todo' + '\n'
                break

        return ret

    # fetches the stats of the cluster
    def fetch_cluster_stats(self,args):
        ret = ''

        for cache_node in self.cache_address:
            main = self.sentinel.main_for(cache_node)
            mem_size = main.info('memory')["used_memory_human"]
            dbsize0 = main.info('keyspace')["db0"]['keys']
            dbsize1 = main.info('keyspace')["db1"]['keys']
            role = main.info('replication')['role']
            subordinate_num = main.info('replication')["connected_subordinates"]
            subordinate_info = main.info('replication')["subordinate0"]
            uptime = main.info('server')["uptime_in_days"]
            ret += '*******************************************' + cache_node + '*******************************************' + '\n'
            ret += 'mem_size:' + mem_size + '\n'
            ret += 'dbsize0:' + str(dbsize0) + ' dbsize1:' + str(dbsize1) + '\n'
            ret += 'role:' + role + '\n'
            ret += 'subordinate_info:' + str(subordinate_info) + '\n'
            ret += 'uptime:'+ str(uptime) + ' days' + '\n'

        return ret

    # Executes redis commands for the cluster
    def execute_command(self,args):
		command_input = args
		command_len=len(command_input.split(' '))
		command=command_input.split(' ')[0]
		args=command_input.split(' ')
		args.remove(command)
		ret = ''

		for cache_node in self.cache_address:
			main = self.sentinel.main_for(cache_node)
			ret += '*******************************************' + cache_node + '*******************************************' + '\n'
			if command_len == 1:
				ret += str(main.execute_command(command)) + '\n'
			elif command_len == 2:
				ret += str(main.execute_command(command,args[0])) + '\n'
			else:
				ret += str(main.execute_command(command,args[0],args[1])) + '\n'

		return ret

    # fetches the cache_info
    def fetch_cache_info(self,args):
		return self.cache_info + '\n'