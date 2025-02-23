import random
from datetime import datetime
from tqdm import tqdm

now = datetime.now()
num = random.randint(1,101)
with open('data/rand.txt','w') as f:
    f.write('{} - Your random number is {}\n'.format(now, num))