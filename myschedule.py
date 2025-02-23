import random
from datetime import datetime
from tqdm import tqdm
import os

# Ensure the "data" folder exists
os.makedirs("data", exist_ok=True)

now = datetime.now()
num = random.randint(1,101)

with open('data/file-out.txt','w') as f:
    f.write('{} - Your random number is {}\n'.format(now, num))