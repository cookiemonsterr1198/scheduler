import random
from datetime import datetime
from tqdm import tqdm

now = datetime.now()
num = random.randint(1,101)
with open('rand.txt','a') as f:
    for i in tqdm(2): 
        f.write('{} - Your random number is {}\n'.format(now, num))