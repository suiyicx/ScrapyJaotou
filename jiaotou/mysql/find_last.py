import re


#截取给定word在s字符串中最后位置以后的字符串并返回
def find_last(word, s): #word like u'a'
    l = [m.start() for m in re.finditer(word, s)]
    if l:
        start = l[-1] + 1
        if len(s) < start:
            return ''
        else:
            return s[start:]
    else:
        return s
