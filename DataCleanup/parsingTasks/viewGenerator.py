__author__ = 'arul'


if __name__ == '__main__':
    newstr=''
    for i in range(1,91):
        newstr += str.format("\"cf\".\"{}\" DOUBLE,",i,i)
    print newstr
