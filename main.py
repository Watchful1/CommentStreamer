import discord_logging
import asyncio
import asyncpraw
from asyncpraw import endpoints
from datetime import datetime, timedelta

log = discord_logging.init_logging()


class ApiRequest:
	def __init__(self, ids):
		self.ids = ids
		self.results = None

	def id_string(self):
		return ','.join(self.ids)


def base36encode(integer: int) -> str:
	chars = '0123456789abcdefghijklmnopqrstuvwxyz'
	sign = '-' if integer < 0 else ''
	integer = abs(integer)
	result = ''
	while integer > 0:
		integer, remainder = divmod(integer, 36)
		result = chars[remainder] + result
	return sign + result


def base36decode(base36: str) -> int:
	return int(base36, 36)


def next_string_id(string_id):
	return base36encode(base36decode(string_id) + 1)


def get_next_hundred_ids(start_id):
	start_num = base36decode(start_id)
	ids = []
	id_num = -1
	for id_num in range(start_num, start_num + 100):
		ids.append("t1_"+base36encode(id_num))
	return ids, base36encode(id_num)


def queue_ids(queue, state, count_sets):
	for _ in range(count_sets):
		ids, last_id_str = get_next_hundred_ids(next_string_id(state.queued_id))
		queue.put_nowait(ids)
		state.queued_id = last_id_str


async def worker(name, queue, reddit):
	while True:
		api_request = await queue.get()
		try:
			response_object = await reddit.get(endpoints.API_PATH["info"], params={"id": api_request.id_string()})
			reddit_objects = response_object.children
			if len(reddit_objects) == 0:
				log.info(f"{name} fetched {api_request.ids[0][3:]} to {api_request.ids[-1][3:]}, got {len(reddit_objects)} objects, requeueing")
				queue.put_nowait(api_request)
			else:
				log.info(f"{name} fetched {api_request.ids[0][3:]} to {api_request.ids[-1][3:]}, got {len(reddit_objects)} objects")
				api_request.results = reddit_objects

		except Exception as e:
			log.info(f"{name} error processing request: {e}")

		queue.task_done()


async def producer(queue, start_id):
	request_list = []
	latest_datetime = None
	while True:
		now = datetime.utcnow()
		log.info(f"producer: {queue.qsize()} : {len(request_list)}")

		if len(request_list):
			api_request = request_list[0]
			if api_request.results is not None:
				latest_datetime = datetime.utcfromtimestamp(api_request.results[-1].created_utc)
				del request_list[0]

		if queue.qsize() < 5:
			if latest_datetime is None or latest_datetime < now - timedelta(seconds=20):
				if len(request_list) == 0:
					queued_id = start_id
				else:
					queued_id = request_list[-1].ids[-1][3:]

				ids, last_id_str = get_next_hundred_ids(next_string_id(queued_id))
				log.info(f"Queueing from {ids[0][3:]} to {ids[-1][3:]}")
				api_request = ApiRequest(ids)
				request_list.append(api_request)
				queue.put_nowait(api_request)

		await asyncio.sleep(0.1)


async def main():
	queue = asyncio.Queue()

	config = discord_logging.get_config()
	user_name = "Watchful12"
	password = discord_logging.get_config_var(config, user_name, "password")
	workers = []
	for i in range(1, 4):
		client_id = discord_logging.get_config_var(config, user_name, f"client_id_{i}")
		client_secret = discord_logging.get_config_var(config, user_name, f"client_secret_{i}")
		reddit = asyncpraw.Reddit(
			username=user_name,
			password=password,
			client_id=client_id,
			client_secret=client_secret,
			user_agent=f"Ingest script {i}")

		task = asyncio.create_task(worker(f'worker-{i}', queue, reddit))
		workers.append(task)
		log.info(f"Initialized worker {i}")

	await producer(queue, "gzy82l7")

	# for task in workers:
	# 	task.cancel()
	# await asyncio.gather(*workers, return_exceptions=True)




	# reddit = asyncpraw.Reddit("Watchful1BotTest")
	#
	# start_id_str = "gmiz119"
	#
	# while True:
	# 	ids, last_id_str = get_next_hundred_ids(start_id_str)
	# 	first_date = None
	# 	last_date = None
	# 	count_found = 0
	#
	# 	submissions = await reddit.get(endpoints.API_PATH["info"], params={"id": ",".join(ids)})
	# 	for submission in submissions:
	# 		count_found += 1
	# 		submission_date = datetime.utcfromtimestamp(submission.created_utc)
	# 		if first_date is None:
	# 			first_date = submission_date
	# 		last_date = submission_date
	# 	first_date_str = first_date.strftime('%Y-%m-%d %H:%M:%S') if first_date is not None else "None"
	# 	last_date_str = last_date.strftime('%Y-%m-%d %H:%M:%S') if last_date is not None else "None"
	# 	log.info(f"{start_id_str} to {last_id_str} : {count_found} : {first_date_str} - {last_date_str}")
	# 	start_id_str = base36encode(base36decode(last_id_str) + 1)


if __name__ == "__main__":
	asyncio.run(main())
