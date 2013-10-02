import hive_utils
import matplotlib.pyplot as plt
import ConfigParser 
import io
import numpy as np

def plot_bar_chart(x_axis,y_axis,x_label,title,file_name):
	y_pos = np.arange(len(y_axis))
	plt.barh(y_pos, x_axis,align='center', alpha=0.4)
	plt.yticks(y_pos, y_axis)
	plt.xlabel(x_label)
	plt.title(title)

 	plt.savefig(file_name)


def df():
	
	config = ConfigParser.RawConfigParser(allow_no_value=True)
	config.read('../config/config.cfg')

	hive_client = hive_utils.HiveClient(
		server  =config.get('Hive','server'),
		port = config.get('Hive','port')
	)

	#Select top n check-in countries
	top10_country = "select country,count(*) as count from checkintest group by country sort by count desc limit 10"
	top10_state_us = "select state,count(*) as count from checkintest where country='US' group by state sort by count desc limit 10"
	top10_city_us = "select city,count(*) as count from checkintest where country='US' group by city sort by count desc limit 10"
 	country_label = []
 	country_cnt = []
 		
 		
 	for row in hive_client.execute(top10_country):
 		country_label.append(row['country'])
 		country_cnt.append(int(row['count']))			
 		
 	
 	us_state_label = []
 	us_state_cnt = []

 	for row in hive_client.execute(top10_state_us):
 		us_state_label.append(row['state'])
 		us_state_cnt.append(int(row['count']))
 	
 	us_city_label = []
 	us_city_cnt = []

 	for row in hive_client.execute(top10_city_us):
 		us_city_label.append(row['city'])
 		us_city_cnt.append(int(row['count']))
 		
 	
 	plot_bar_chart(country_cnt,country_label,'Check-in Count','Top 10 Check-in countries','top10_country.png')
 	plot_bar_chart(us_state_cnt,us_state_label,'Check-in Count','Top 10 Check-in states in US','top10_state_us.png')
 	plot_bar_chart(us_city_cnt,us_city_label,'Check-in Count','Top 10 Check-in cities in US','top10_city_us.png')
 
 
	    

if __name__ == '__main__':
	df()
