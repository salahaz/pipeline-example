from crontab import CronTab

cron = CronTab(user=True)
job = cron.new(command='python3 /Users/salahazekour/Documents/SP20/Data/Project1/local_script.py')
job.minute.every(1)

cron.write()
