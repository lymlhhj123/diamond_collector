#-*-coding:utf-8-*-
import diamond
import redis
import json

class ScrapyCollector(diamond.collector.Collector):

	def get_default_config_help(self):

		config_help = super(ScrapyCollector, self).get_default_config_help()
		config_help.update({'host': 'redis host',
			                'port': 'redis port',
			                'db': 'redis database',
			                'password': 'redis password',})

		return config_help

	def get_default_config(self):
		config = super(ScrapyCollector, self).get_default_config()

		d = {
		     'hostname': 'ubuntu_1',
		     'enabled': True,
		     #'path': '',
		     'path_prefix': 'scrapy',
		     'path_suffix': '',
		     'interval': 70,
		     'hostname_method': 'smart',
		     'measure_collector_time': True,
		     #reids连接设置
		     'host': '192.168.202.130', 
		     'port': 6379,
		     'password': 'lymlhhj123',
		     'db': 0,}

		config.update(d)
		return config

	def collect(self):
		rc = redis.StrictRedis(host=self.config['host'],
			                   port=self.config['port'],
			                   password=self.config['password'],
			                   db=self.config['db'],)

		result = rc.rpop('scrapy_stats')
		if result:
			x = json.loads(result if isinstance(result, str) 
								     else result.decode())

			for k, v in x.items():
				self.publish(k, v)

		#断开redis的连
		rc.connection_pool.disconnect()
