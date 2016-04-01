# -*- coding: utf-8 -*-
import Configurations
import time
'''
@author: Administrator
'''
import operator

class DssDataManager:

    configurations = None
    cache_info=''
    sentinel_address=''
    cache_address=''
    seninel_ip=''
    seninel_port=''
    sentinel=None
    sentinel_db1=None

    def init_config(self):

        self.cache_info=self.configurations.cache_info

        self.sentinel_address=self.configurations.sentinel_address
        self.cache_address=self.configurations.cache_address
        print  'cc',self.cache_address
        self.seninel_ip=self.configurations.seninel_ip
        self.seninel_port=self.configurations.seninel_port
        self.sentinel=self.configurations.sentinel
        self.sentinel_db1=self.configurations.sentinel_db1

    def set_configurations(self,configurations_out):
        self.configurations = configurations_out

        #self.configurations.init_config()
        self.init_config()
        print 'pppppppp',self.configurations.cache_info
        print 'pppppppp',configurations_out.cache_info

    #测试web
    def testWeb(self):
        s = self.cache_info + '\n'
        for i in range(0,6):
            s += 'I love ' + str(i) + '\n'
        return s

    #查找cache集群的总的mstpid个数
    def findAllMstpIdNum(self,args):
         
        all_num_mstpid = 0
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            mstpid_list = master.keys('*')
            all_num_mstpid += len(mstpid_list)
        #print 'xx',all_num_mstpid
        return str(all_num_mstpid)

    #查找、统计seqId个数在指定范围内的mstpid
    def findMstpIdWhoseSeqIdIsInSection(self,args):
    	print args
    	begin_num = int(args.split(' ')[0])
    	end_num = int(args.split(' ')[1])
        #begin_num = int(raw_input('Please input the begin number of seqId:\n'))
        #end_num = int(raw_input('Please input the end number of seqId:\n'))
        ret = ''
        if end_num<=begin_num:
            ret += 'Error Input!'
            return ret
        count = 0
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            mstpid_list = master.keys('*')
            
            pipe = master.pipeline()
            for mstpid in mstpid_list:
                hlen = pipe.hlen(mstpid)
            lenlist = pipe.execute()
            i = 0
            while i < len(lenlist):
                if begin_num < lenlist[i] < end_num:
                    count += 1
                    ret += mstpid_list[i] + ' ' + str(lenlist[i]) + '\n'
                i += 1
                #print mstpid_len
                #print type(begin_num)
                
        #print count*1.0/int(self.findAllMstpIdNum(args))*100
        ret += 'Rate is %.2f%%' % (count*1.0/int(self.findAllMstpIdNum(args))*100)
        return ret
        

    #清库 慎用
    def flushDB(self):
        for cache_node in cache_address:
            print cache_node
        choice = raw_input("Please input the cache address(like 'ip:port') you want to flush. 'all' represents flush all cach nodes")
        
        if choice != all:
            confirm = raw_input('Are you sure to flushDB the cache node? yes/no')
            if confirm != 'yes':
                exit()
            master0 = sentinel.master_for(choice)
            master1= sentinel_db1.master_for(choice)
            master0.flushDB()
            master1.flushDB()
        else:
            confirm = raw_input('Are you sure to flushDB all cache nodes? yes/no')
            if confirm != 'yes':
                exit()
            for cache_node in cache_address:
                master0 = sentinel.master_for(cache_node)
                master1= sentinel_db1.master_for(cache_node)
                master0.flushDB()
                master1.flushDB()
            
        for cache_node in cache_address:
            master0 = sentinel.master_for(cache_node)
            master1= sentinel_db1.master_for(cache_node)
            print cache_node,'\t',master0.dbsize(),'\t',master1.dbsize()

    #打印目前进些写操作的客户端ip
    def printWriteClient(self,args):
        #while True:
        ret = ''
        for cache_node in self.cache_address:
        	master = self.sentinel.master_for(cache_node)
        	client_list = master.client_list()
        	ip_set = set()
        	for client in client_list:
        		cmd = client['cmd']
        		idle = client['idle']
        		if(cmd == 'set' or cmd == 'hset') and idle < 2:
        			ip_set.add(client['addr'].split(':')[0])
        			ret += ip_set + '\n'    
        	ret += '*******************************************' + '\n'
        return ret
                    
                    
                    
        

    #打印集群上所有mstpid下的seqid数量 
    def findSeqNumOfAllMstpid(self,args):
        ret = ''
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            #start=time.time()
            mstpid_list = master.keys('*')
            #end=time.time()
            #cost = end - start
            #print cost
            #print mstpid_list.__sizeof__()
            pipe = master.pipeline()
            #print type(pipe),pipe.__dict__
            for mstpid in mstpid_list:
            	hlen = pipe.hlen(mstpid)
            lenlist = pipe.execute()
            for i in range(len(mstpid_list)):
                ret += str(mstpid_list[i]) + '\t' + str(lenlist[i]) + '\n' 
            #print lenlist
                #seqid_list = master.hkeys(mstpid)
            #dict.setdefault(mstpid,hlen)
            #del mstpid_list

        #sorted_dict = sorted(dict.iteritems(), key=operator.itemgetter(1), reverse=True)    #根据seqid数量排序 默认升序
        return ret
                
                
    #删除集群中所有哈希存储结构为hashtable而不是ziplist的mstpid
    def deleteHashMstpId(self):
        hash_mstpid_dict = findHashMstpId()
        confirm=raw_input('Are you sure to delete?(yes/no)\n')
        if confirm != 'yes':
            exit()
        for item in hash_mstpid_dict:
            master = sentinel_db1.master_for(hash_mstpid_dict[item])
            master.delete(item)
        print 'deleted num:',len(hash_mstpid_dict)

     #查找集群中所有哈希存储结构为hashtable而不是ziplist的mstpid，以及其比例
    def findHashMstpId(self,args):
        findsets = {}
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            mstpid_list = master.keys('*')
            pipe = master.pipeline()
            for mstpid in mstpid_list:
                pipe.object('encoding',mstpid)
            typelist = pipe.execute()
            for i in range(len(typelist)):
                if typelist[i] == 'hashtable':
                    #print mstpid, cache_node
                    findsets.setdefault(mstpid_list[i],cache_node)
        ret = ''
        ret += 'Rate is ' + str(len(findsets)*1.0/int(self.findAllMstpIdNum(args))*100) + '%' + '\n'
        for i in findsets:
            ret +=  i + '\t' + str(findsets[i]) + '\n'
        return ret

    #查找某个mstpid的内容
    def findContentOfMstpId(self,args):
    	ret = ''
        #mstpid_input = raw_input('Please input the mstpid:\n')
        mstpid_input = args
        isExists,location = self.findWhereIsMstpId(mstpid_input)
        if isExists == False:
            return 'The mstpId is not exist!'
        master = self.sentinel_db1.master_for(location)
        content = master.hgetall(mstpid_input)  #返回乱序，因为python没有ziplist结构,存储成字典后因为哈希值乱序
        data_type = master.object('encoding',mstpid_input)
        master0 = self.sentinel.master_for(location)
        latest_seqId_0 = master0.get(mstpid_input.split('MSTPID')[0]+'SEQ_0')
        latest_seqId_1 = master0.get(mstpid_input.split('MSTPID')[0]+'SEQ_1')
        latest_ackId_0 = master0.hget(mstpid_input.split('MSTPID')[0]+'ACK',0)
        latest_ackId_1 = master0.hget(mstpid_input.split('MSTPID')[0]+'ACK',1)
        
        ret += 'location:' + location + '\n'
        ret += 'latest seqId 0:' + str(latest_seqId_0) + '\tlatest seqId 1:' + str(latest_seqId_1) + '\n'
        ret += 'latest ackId 0:' + str(latest_ackId_0) + '\tlatest ackId 1:' + str(latest_ackId_1) + '\n'
        ret += 'data type:' + str(data_type) + '\n'
        ret += 'item num:' + str(len(content)) + '\n'
        ret += 'contents:' + '\n'
        seqid_list = master.hkeys(mstpid_input)
        for item in seqid_list:
            ret += item + '\t' + content[item] + '\n'
        return ret
        

    #查找是否存某个msgid及其位置
    def findWhereIsMsgId(self,args):
    	msgId = args
    	ret = ''
        isExists = False
        for cache_node in self.cache_address:
            master = self.sentinel.master_for(cache_node)
            isExists = master.exists(msgId)
            if isExists == True:
                location = cache_node
                ret += 'msgId location:' + cache_node
                break
        if isExists == False:
        	ret += 'can not find msgId:' + msgId
        return ret
        #return isExists,location

    #查找是否存在某个mstpid及其位置
    def findWhereIsMstpId(self,mstpid):
        isExists = False
        location = ''
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            isExists = master.exists(mstpid)
            if isExists == True:
                location= cache_node
                print 'mstpid location:',cache_node
                break
        if isExists == False:
                print 'can not find mstpid:',mstpid
        return isExists,location

    #查找指定mstpid和seqid下的msgid与对应msgbody
    def findMsg(self,args):
        #mstpid_input = raw_input('Please input the mstpid:\n')
        #seqid_input = raw_input('Please input the seqid:\n')
        mstpid_input = args.split(' ')[0]
    	seqid_input = args.split(' ')[1]
    	ret = ''
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            msgid = master.hget(mstpid_input,seqid_input)
            if msgid != None:
                ret += 'mstpid location:' + cache_node + '\n'
                break
        if msgid == None:
            ret += 'can not find the msgid you want\n'  
            return ret 
        for cache_node in self.cache_address:
            master = self.sentinel.master_for(cache_node)
            msgbody = master.get(msgid)
            if msgbody != None:
                ret += 'message location:' + cache_node + '\n'
                ret += 'msgid:' + msgid + '\n'
                ret += 'msgbody:\n' + 'todo' + '\n'
                break
        return ret

    def clusterStats(self,args):
        ret = ''
        print len(self.cache_address)
        for cache_node in self.cache_address:
            master = self.sentinel.master_for(cache_node)
            mem_size = master.info('memory')["used_memory_human"]
            dbsize0 = master.info('keyspace')["db0"]['keys']
            dbsize1 = master.info('keyspace')["db1"]['keys']
            role = master.info('replication')['role']
            slave_num = master.info('replication')["connected_slaves"]
            slave_info = master.info('replication')["slave0"]
            uptime = master.info('server')["uptime_in_days"]
            ret += '*******************************************' + cache_node + '*******************************************' + '\n'
            ret += 'mem_size:' + mem_size + '\n'
            ret += 'dbsize0:' + str(dbsize0) + ' dbsize1:' + str(dbsize1) + '\n'
            ret += 'role:' + role + '\n'
            #print 'slave_num:',slave_num
            ret += 'slave_info:' + str(slave_info) + '\n'
            ret += 'uptime:'+ str(uptime) + ' days' + '\n'
        #print ret
        return ret

    def commandExecutor(self,args):
		command_input = args
		#command_input=raw_input('Please input the command you want to execute:\n')
		command_len=len(command_input.split(' '))
		command=command_input.split(' ')[0]
		args=command_input.split(' ')
		args.remove(command)
		ret = ''
		for cache_node in self.cache_address:
			master = self.sentinel.master_for(cache_node)
			ret += '*******************************************' + cache_node + '*******************************************' + '\n'
			if command_len == 1:
				ret += str(master.execute_command(command))+'\n'
			elif command_len == 2:
				ret += str(master.execute_command(command,args[0]))+'\n'
			else:
				ret += str(master.execute_command(command,args[0],args[1])) + '\n'
		return ret

    def cache_info_export(self,args):
		
		return self.cache_info + '\n'