import math
import re
import random
import io
import codecs

def factorize(num):
    for n in range(2, int(math.sqrt(num))):
        if num % n == 0:
            yield n

def queen(n, m, qx, qy):
    field = list()

    for py in range(m):
        row = ['.' for _ in range(n)]
        for px in range(n):
            if (py, px) == (qy, qx):
                row[px] = 'D'
            elif abs(qy - py) == abs(qx - px) or py == qy or px == qx:
                row[px] = '*'
        
        field.append(row)
    return field
            
def censor_number(n, m):
    num = list()
    for x in range(n):
        num.append(x if m != x else '*')
    return list(map(str, num))

def text_analysis(path):
    with open(path, "r") as f:
        content = f.read()
    
    lit, word = dict(), dict()
    for c in content:
        lc = lit.get(c) or 0
        lit[c] = lc + 1
    
    content = map(str.lower, re.split(r"[^\w]+", content, flags=re.U))
    for w in content:
        wc = word.get(w) or 0
        word[w] = wc + 1
    
    return lit, word

def get_words(n, m, ta_tup):
    words = list()
    for w in list(ta_tup[1].keys()):
        if len(w) < m:
            continue

        if len(words) < n:
            words.append(w)
        else:
            break
    
    return words

def cypher(fin, fout, key = 10):
    with open(fin, "r") as f:
        content = f.read()

    with open(fout, "wb") as f:
        for c in content:
            f.write(random.randbytes(key - 1))
            f.write(c.encode())
        f.write(random.randbytes(key))

def decypher(fin, fout, key = 10):
    with open(fin, "rb") as f:
        content = f.read()
    
    with open(fout, "w") as f:
        f.write("".join(
            map(lambda it: chr(it[1]), 
            filter(lambda it: (it[0] + 1) % key == 0, enumerate(content)))
        )[:-1])

cypher("test.txt", "test.bin")
decypher("test.bin", "test.dec")