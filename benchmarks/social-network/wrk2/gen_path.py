import sys;
import os;

with open("paths.txt", "w") as of:
    n = 1000;

    n = int(sys.argv[1]);

    for i in range(n):
        of.write("/%d.html\n" % (i))
