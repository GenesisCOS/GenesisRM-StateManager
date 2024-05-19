import numpy as np 
import random 


WARMUP_LEN = 180
WORKLOAD_LEN = 3600

def scale_nasa_min_(x):
    data = list()
    with open('nasa_1day_6hour.txt', 'r') as file:
        for line in file.readlines():
            if line == '\n':
                continue
            data.append(int(line.strip('\n')))
    result = list()
    for i in data:
        result.append(i if i > x else x)
    with open(f'nasa_1day_6hour_min_{x}.txt', 'w+') as file:
        for i in result:
            file.write(f'{int(i)}\n')
            file.flush()

def scale_nasa():
    # scale NASA dataset from 1 day 60min to 1 day 6hour
    data = list()
    with open('nasa_1day_60min.txt', 'r') as file:
        for line in file.readlines():
            if line == '\n':
                continue
            data.append(int(line.strip('\n')))
    result = list()
    for i in data:
        for _ in range(6):
            result.append(i)
    with open('nasa_1day_6hour.txt', 'w+') as file:
        for i in result:
            file.write(f'{int(i)}\n')
            file.flush()

def gen_bursty():
    data = list()
    with open('bursty.txt', 'r') as file:
        for line in file.readlines():
            if line == '\n':
                continue
            data.append(int(line.strip('\n')))
    data = np.array(data)
    for i in range(len(data)):
        if data[i] < 100:
            data[i] == 100
    max_value = np.max(data)
    plus = 1000 / max_value
    data = data * plus 
    with open('bursty_me.txt', 'w+') as file:
        for _ in range(WARMUP_LEN):
            file.write(f'{100}\n')
            file.flush()
        for i in data:
            file.write(f'{int(i)}\n')
            file.flush()

def gen_noisy():
    with open('noisy.txt', 'w+') as file:
        for _ in range(WARMUP_LEN):
            file.write(f'{100}\n')
            file.flush()
        
        prev = 800
        count = 0
        for _ in range(WORKLOAD_LEN):
            if count == 0:
                __prev = prev 
                prev = int(random.random() * 900 + 100)
                prev = max(__prev - 300, prev)
                prev = min(__prev + 300, prev)

            file.write(f'{prev}\n')
            file.flush()
            if count > 10:
                count = 0
            else:
                count += 1

 
def gen_const(number: int):
    with open(f'const_{number}.txt', 'w+') as file:
        for _ in range(1 * 60 * 60):
            file.write(f'{number}\n')
            file.flush()


if __name__ == '__main__':
    scale_nasa_min_(900)

