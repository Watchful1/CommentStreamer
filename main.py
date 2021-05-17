import discord_logging
import asyncio
import asyncpraw
from asyncpraw import endpoints
from datetime import datetime

log = discord_logging.init_logging()


class ObjectRequest:
	def __init__(self, fullname):
		self.fullname = fullname
		self.filled = False
		self.result = None

	def __str__(self):
		return self.fullname


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


def get_next_hundred_ids(start_id):
	start_num = base36decode(start_id)
	ids = []
	id_num = -1
	for id_num in range(start_num, start_num + 100):
		ids.append("t1_"+base36encode(id_num))
	return ids, base36encode(id_num)


async def worker(name, queue):
	while True:
		# Get a "work item" out of the queue.
		sleep_for = await queue.get()

		# Sleep for the "sleep_for" seconds.
		await asyncio.sleep(sleep_for)

		# Notify the queue that the "work item" has been processed.
		queue.task_done()

		print(f'{name} has slept for {sleep_for:.2f} seconds')


async def main():
	queue = asyncio.Queue()

	# Generate random timings and put them into the queue.
	total_sleep_time = 0
	for _ in range(20):
		sleep_for = random.uniform(0.05, 1.0)
		total_sleep_time += sleep_for
		queue.put_nowait(sleep_for)

	# Create three worker tasks to process the queue concurrently.
	tasks = []
	for i in range(3):
		task = asyncio.create_task(worker(f'worker-{i}', queue))
		tasks.append(task)

	# Wait until the queue is fully processed.
	started_at = time.monotonic()
	await queue.join()
	total_slept_for = time.monotonic() - started_at

	# Cancel our worker tasks.
	for task in tasks:
		task.cancel()
	# Wait until all worker tasks are cancelled.
	await asyncio.gather(*tasks, return_exceptions=True)




	reddit = asyncpraw.Reddit("Watchful1BotTest")

	start_id_str = "gmiz119"

	while True:
		ids, last_id_str = get_next_hundred_ids(start_id_str)
		first_date = None
		last_date = None
		count_found = 0

		submissions = await reddit.get(endpoints.API_PATH["info"], params={"id": ",".join(ids)})
		for submission in submissions:
			count_found += 1
			submission_date = datetime.utcfromtimestamp(submission.created_utc)
			if first_date is None:
				first_date = submission_date
			last_date = submission_date
		first_date_str = first_date.strftime('%Y-%m-%d %H:%M:%S') if first_date is not None else "None"
		last_date_str = last_date.strftime('%Y-%m-%d %H:%M:%S') if last_date is not None else "None"
		log.info(f"{start_id_str} to {last_id_str} : {count_found} : {first_date_str} - {last_date_str}")
		start_id_str = base36encode(base36decode(last_id_str) + 1)


if __name__ == "__main__":
	asyncio.run(main())
