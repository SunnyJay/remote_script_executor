# -*- coding: utf-8 -*-
import Configurations
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

    #����web
    def testWeb(self):
        s = self.cache_info + '\n'
        for i in range(0,6):
            s += 'I love ' + str(i) + '\n'
        return s

    #����cache��Ⱥ���ܵ�mstpid����
    def findAllMstpIdNum(self,args):
         
        all_num_mstpid = 0
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            mstpid_list = master.keys('*')
            all_num_mstpid += len(mstpid_list)
        #print 'xx',all_num_mstpid
        return str(all_num_mstpid)

    #���ҡ�ͳ��seqId������ָ����Χ�ڵ�mstpid
    def findMstpIdWhoseSeqIdIsInSection(self,args):
    	print args
    	begin_num = args.split(' ')[0]
    	end_num = args.split(' ')[1]
        #begin_num = int(raw_input('Please input the begin number of seqId:\n'))
        #end_num = int(raw_input('Please input the end number of seqId:\n'))
        ret = ''
        if end_num<=begin_num:
            ret += 'Error Input!'
            exit()
        count = 0
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            mstpid_list = master.keys('*')
            
            for mstpid in mstpid_list:
                mstpid_len =  master.hlen(mstpid)
                if begin_num < mstpid_len < end_num:
                    count += 1
                    print mstpid,master.hlen(mstpid)
        ret += 'Rate is ' + str(count*1.0/int(self.findAllMstpIdNum(args))*100) + '%'
        return ret
        

    #��� ����
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

    #��ӡĿǰ��Щд�����Ŀͻ���ip
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
                    
                    
                    
        

    #��ӡ��Ⱥ������mstpid�µ�seqid���� 
    def findSeqNumOfAllMstpid(self,args):
        dict = {}
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            mstpid_list = master.keys('*')
            for mstpid in mstpid_list:
                seqid_list = master.hkeys(mstpid)
                dict.setdefault(mstpid,len(seqid_list))
        sorted_dict = sorted(dict.iteritems(), key=operator.itemgetter(1), reverse=True)    #����seqid�������� Ĭ������
        ret = ''
        for item in sorted_dict:
            ret += str(item[0]) + '\t' + str(item[1]) + '\n'
        return ret
                
                

    #ɾ����Ⱥ�����й�ϣ�洢�ṹΪhashtable������ziplist��mstpid
    def deleteHashMstpId(self):
        hash_mstpid_dict = findHashMstpId()
        confirm=raw_input('Are you sure to delete?(yes/no)\n')
        if confirm != 'yes':
            exit()
        for item in hash_mstpid_dict:
            master = sentinel_db1.master_for(hash_mstpid_dict[item])
            master.delete(item)
        print 'deleted num:',len(hash_mstpid_dict)

    #���Ҽ�Ⱥ�����й�ϣ�洢�ṹΪhashtable������ziplist��mstpid���Լ������
    def findHashMstpId(self,args):
        findsets = {}
        for cache_node in self.cache_address:
            master = self.sentinel_db1.master_for(cache_node)
            mstpid_list = master.keys('*')
            
            for mstpid in mstpid_list:
                data_type = master.object('encoding',mstpid)
                if data_type == 'hashtable':
                    print mstpid, cache_node
                    findsets.setdefault(mstpid,cache_node)
        ret = ''
        ret += 'Rate is ' + str(len(findsets)*1.0/int(self.findAllMstpIdNum(args))*100) + '%' + '\n'
        ret +=  str(findsets)
        return ret

    #����ĳ��mstpid������
    def findContentOfMstpId(self,args):
    	ret = ''
        #mstpid_input = raw_input('Please input the mstpid:\n')
        mstpid_input = args
        isExists,location = self.findWhereIsMstpId(mstpid_input)
        if isExists == False:
            return 'The mstpId is not exist!'
        master = self.sentinel_db1.master_for(location)
        content = master.hgetall(mstpid_input) #����������Ϊpythonû��ziplist�ṹ,�洢���ֵ����Ϊ��ϣֵ����
        data_type = master.object('encoding',mstpid_input)
        master0 = self.sentinel.master_for(location)
        latest_seqId_0 = master0.get(mstpid_input.split('MSTPID')[0]+'SEQ_0')
        latest_seqId_1 = master0.get(mstpid_input.split('MSTPID')[0]+'SEQ_1')
        latest_ackId_0 = master0.hget(mstpid_input.split('MSTPID')[0]+'ACK',0)
        latest_ackId_1 = master0.hget(mstpid_input.split('MSTPID')[0]+'ACK',1)
        
        ret += 'location:' + location + '\n'
        ret += 'latest seqId 0:' + latest_seqId_0 + '\tlatest seqId 1:' + latest_seqId_1 + '\n'
        ret += 'latest ackId 0:' + latest_ackId_0 + '\tlatest ackId 1:' + latest_ackId_1 + '\n'
        ret += 'data type:' + str(data_type) + '\n'
        ret += 'item num:' + str(len(content)) + '\n'
        ret += 'contents:' + '\n'
        seqid_list = master.hkeys(mstpid_input)
        for item in seqid_list:
            ret += item + '\t' + content[item] + '\n'
        return ret
        

    #�����Ƿ��ĳ��msgid����λ��
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

    #�����Ƿ����ĳ��mstpid����λ��
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

    #����ָ��mstpid��seqid�µ�msgid���Ӧmsgbody
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