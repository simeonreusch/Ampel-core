
from ampel.abstract.AbsTargetSource import AbsTargetSource

import pytest
import logging
try:
	from astropy import units as u
	from astropy.time import Time
except ImportError:
	pytest.skip("Delayed T0 requires astropy", allow_module_level=True)
from numpy import random, arccos
random.seed(0)
import asyncio

class PotemkinTargetSource(AbsTargetSource):
	
	version = 0.1
	
	def __init__(self, idx):
		self.idx = idx
	
	async def get_targets(self):
		for i in range(2):
			await asyncio.sleep(random.uniform(0, 1))
			t0 = Time.now()
			yield ((random.uniform(-180, 180)*u.deg, (arccos(random.uniform(0, 1))*u.rad).to(u.deg)), 0.1*u.deg*self.idx, (t0-5*u.day, t0+25*u.day), ['NO_FILTER'])

class TargetSourceListener(AbsTargetSource):
	"""
	Listen for new sources on a socket
	
	Feed with e.g.: echo '141 45 2.5 2018-06-22T12:00:00 2018-06-23T12:00:00 NO_FILTER' | nc localhost 12345
	"""
	
	version = 0.1
	
	def __init__(self):
		self._queue = asyncio.Queue(maxsize=0)

	async def handle_connection(self, reader, writer):
		line = await reader.readline()
		try:
			fields = line.decode().strip().split(' ')
			if len(fields) < 6:
				raise ValueError('Too few fields')
			ra, dec, radius = map(float, fields[:3])
			jd_min, jd_max = fields[3:5]
			channels = fields[5:]
			target = ((ra*u.deg, dec*u.deg), radius*u.deg, Time(jd_min), Time(jd_max), channels)
			await self._queue.put(target)
			writer.write('{}\n'.format(target).encode())
		except Exception as e:
			writer.write((str(e)+'\n').encode())
		finally:
			await writer.drain()
			writer.close()

	async def get_targets(self):
		print('going to start')
		server = await asyncio.start_server(self.handle_connection, '127.0.0.1', 12345)
		while True:
			yield await self._queue.get()
		
		server.close()
		await server.wait_closed()

from ampel.ztf.archive.ArchiveDB import ArchiveDB
from sqlite3 import OperationalError

@pytest.fixture
def archivedb():
	uri = 'postgresql://ampel-readonly:XXXXXXXXX@localhost:5432/ztfarchive'
	try:
		ArchiveDB(uri)
		return uri
	except OperationalError as e:
		raise e # TODO: return won't be called after this
		return pytest.skip("No archive db found")

from ampel.config.AmpelConfig import AmpelConfig
@pytest.fixture
def testing_config(mongod, archivedb):
	AmpelConfig.reset()
	config = {
	    'resources': {
	        'mongo': {'writer': mongod},
	        'archive': {'reader': archivedb},
	        },
	    'channels': [
			{
				"channel": "NO_FILTER",
	            "active": False,
	            "sources": [
					{
						"stream": "ZTFIPAC",
	                    "parameters": {
	                        "ZTFPartner": True,
	                        "auto_complete": True,
	                        "updatedHUZP": True
	                    },
	                    "t0_filter": {
	                        "className": "NoFilter",
	                        "run_config": {}
	                    },
	                    "t2_compute": []
	                }
	            ]
	        },
	    ],
	    't0_filters' : {
	        'NoFilter' : {
	            'classFullPath': 'ampel.contrib.hu.t0.NoFilter',
	        },
	    },
	    't2Units': {},
	}
	AmpelConfig.set_config(config)
	yield config
	AmpelConfig.reset()

from ampel.t0.DelayedT0Controller import DelayedT0Controller

def test_source():
	ts = PotemkinTargetSource(0)
	
	async def getem():
		async for t in ts.get_targets():
			assert isinstance(t, tuple)
	asyncio.get_event_loop().run_until_complete(getem())

@pytest.mark.skip
def test_source_listener():
	
	listener = TargetSourceListener()
	async def getem():
		async for t in listener.get_targets():
			assert isinstance(t, tuple)
			print(repr(t))
	
	asyncio.get_event_loop().run_until_complete(getem())

@pytest.mark.skip
def test_delayed_controller(testing_config, caplog):
	caplog.set_level('INFO')
	
	controller = DelayedT0Controller([PotemkinTargetSource(i) for i in range(3)])
	
	controller.run()