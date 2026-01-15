import time
import random
import string

def now() -> float:
    return time.time()

def gen_id(prefix: str) -> str:
    suf = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
    return f"{prefix}_{int(time.time()*1000)}_{suf}"
