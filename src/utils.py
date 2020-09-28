import re


def getstocklist(filename):
    lines = open(filename).read().splitlines()
    flist = [x for x in lines if isvalid(x)]
    return flist


def isvalid(x):
    skip = (
        re.search(r"^\s*#", x) or
        re.search(r"^\s*$", x)
    )
    return not skip
