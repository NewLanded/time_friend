# time_friend

pip install:
    schedule
    pandas
    asyncpg
    TA-lib

nohup /home/stock/anaconda3/envs/stock/bin/python /home/stock/app/time_friend/timed_task.py > /dev/null 2>&1 &
