#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/common/AmpelExceptionPublisher.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 03.09.2018
# Last Modified Date: 04.09.2018
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import datetime, json, logging, time

from ampel.db.AmpelDB import AmpelDB
from ampel.config.AmpelConfig import AmpelConfig

from slackclient import SlackClient
from bson import ObjectId

log = logging.getLogger()

class AmpelExceptionPublisher:
	def __init__(self, dry_run=False, user='AMPEL-live', channel='ampel-troubles'):
		token = AmpelConfig.get('resource.slack.operator')
		self._slack = SlackClient(token)
		self._troubles = AmpelDB.get_collection('troubles', 'r')
		self._dry_run = dry_run
		self._user = user
		self._channel = channel

		self._last_timestamp = ObjectId.from_datetime(datetime.datetime.now() - datetime.timedelta(hours=1))

	def t3_fields(self, doc):
		fields = []
		if 'job' in doc:
			fields.append({'title': 'Job', 'value': doc.get('job', None), 'short': True})
		if 'task' in doc:
			fields.append({'title': 'Task', 'value': doc.get('task', None), 'short': True})
		fields.append({'title': 'Run', 'value': doc.get('run', None), 'short': True})
		return fields

	def format_attachment(self, doc):
		fields = [{'title': 'Tier', 'value': doc['tier'], 'short': True}]
		more = doc.get('more', {})
		if doc['tier'] == 0:
			for field in 'section', 'stock', 'run':
				fields.append({'title': field, 'value': doc.get(field, None), 'short': True})
			if 'id' in doc.get('alert', {}):
				fields.append({'title': 'alertId', 'value': doc.get('alert', {}).get('id', None), 'short': True})
		elif doc['tier'] == 2:
			fields.append({'title': 'unit', 'value': doc.get('unit', None), 'short': True})
			fields.append({'title': 'run', 'value': doc.get('run', None), 'short': True})
			t2Doc = doc.get('t2Doc', None)
			if hasattr(t2Doc, 'binary'):
				fields.append({'title': 't2Doc', 'value': t2Doc.binary.hex(), 'short': True})
		elif doc['tier'] == 3:
			fields += self.t3_fields(more if 'jobName' in more else doc)
		if 'exception' in doc:
			text =  '```{}```'.format('\n'.join(doc['exception']))
		elif 'location' in doc:
			text = '{}: {}'.format(doc['location'], doc.get('ampelMsg', ''))
			if 'mongoUpdateResult' in doc:
				text += '\nmongoUpdateResult: `{}`'.format(doc['mongoUpdateResult'])
			elif 'errDict' in doc:
				text += '```\n{}```'.format(repr(doc['errDict']))
		else:
			text = 'Unknown exception type. Doc keys are: ```{}```'.format(doc.keys())

		attachment = {
			'fields': fields,
			'ts': int(doc['_id'].generation_time.timestamp()),
			'text': text,
			'mrkdwn_in': ['text'],
		}
		return attachment

	def publish(self):

		message = {
			'attachments': [],
			'channel': '#'+self._channel,
			'username': self._user,
			'as_user': False
		}

		projection = ['_id', 'exception']
		t0 = self._last_timestamp.generation_time
		cursor = self._troubles.find({'_id': {'$gt': self._last_timestamp}})
		for doc in cursor:
			if len(message['attachments']) < 20:
				message['attachments'].append(self.format_attachment(doc))

		try:
			self._last_timestamp = doc['_id']
			dt = ObjectId.from_datetime(datetime.datetime.now()).generation_time - t0
			if dt.days > 3:
				time_range = '{} days'.format(dt.days)
			elif dt.days > 0 or dt.seconds > 2*3600:
				time_range = '{} hours'.format(int(dt.days * 24 + dt.seconds / 3600))
			elif dt.seconds > 2*60:
				time_range = '{} minutes'.format(int(dt.seconds/60))
			else:
				time_range = '{} seconds'.format(int(dt.seconds))
		except UnboundLocalError:
			if self._dry_run:
				log.info("No exceptions")
			return

		count = cursor.count()
		if len(message['attachments']) < count:
			message['text'] = 'Here are the first {} exceptions. There were {} more in the last {}.'.format(len(message['attachments']), count-len(message['attachments']), time_range)
		else:
			message['text'] = 'There were {} exceptions in the last {}.'.format(len(message['attachments']), time_range)

		if self._dry_run:
			log.info(json.dumps(message, indent=1))
		else:
			result = self._slack.api_call('chat.postMessage', **message)
			if not result['ok']:
				raise RuntimeError(result['error'])
		log.info("{} exceptions in the last {}".format(count, time_range))

def run():
	import schedule
	from ampel.run.AmpelArgumentParser import AmpelArgumentParser
	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['logger'])
	parser.require_resource('slack', ['operator'])
	parser.add_argument('--interval', type=int, default=10, help='Check for new exceptions every INTERVAL minutes')
	parser.add_argument('--channel', type=str, default='ampel-troubles', help='Publish to this Slack channel')
	parser.add_argument('--user', type=str, default='AMPEL-live', help='Publish to as this username')
	parser.add_argument('--dry-run', action='store_true', default=False,
	    help='Print exceptions rather than publishing to Slack')

	args = parser.parse_args()

	atp = AmpelExceptionPublisher(dry_run=args.dry_run, channel=args.channel, user=args.user)
	scheduler = schedule.Scheduler()
	scheduler.every(args.interval).minutes.do(atp.publish)

	logging.basicConfig()
	atp.publish()
	while True:
		try:
			scheduler.run_pending()
			time.sleep(10)
		except KeyboardInterrupt:
			break