import hive_utils
#import pylab
import matplotlib.pyplot as plt
import ConfigParser 
import io
def cdf():
	
	config = ConfigParser.RawConfigParser(allow_no_value=True)
	config.read('../config/config.cfg')

	hive_client = hive_utils.HiveClient(
		server  =config.get('Hive','server'),
		port = config.get('Hive','port')
	)


 	base_query = 'select count(*) as cnt from (select count(*) as num from checkin group by user_id) a where a.num > '

 	ax = []
 	ay = []

 	st = 5
 	tot_user = 2286501.0

 	while True:
 		
 		count_query = base_query + str(st)
 		cnt_int = 0

 		for row in hive_client.execute(count_query):
 			ax.append(st)
 			cnt_per = float(row['cnt'])/tot_user
 			#print type(row['cnt'])
 			ay.append(cnt_per) 
 			print row['cnt'],cnt_per
 			
 		
 		st = st + 50

 		if int(row['cnt'])<=1:
 			break
 		
 		
 		print count_query
 		
 		
 	fig, axi = plt.subplots()
 	axi.set_yscale("log")
 	axi.set_xscale("log")
 	axi.set_ylim(0, 1e6)
 	axi.set_ylim(1e-10, 1e0)
 	axi.set_xlabel('Number of checkins per user')
 	axi.set_ylabel('CCDF')

 	plt.plot(ax,ay,'o')	
 	plt.savefig('cdf_test.png')

	#query  = "select * from checkin where user_id='750'"
	# for row in hive_client.execute(query):
	# 	print '%s: %s' %(row['text'],row['usr_name'])
	    

if __name__ == '__main__':
	cdf()
