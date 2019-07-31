import sys
import arrow
import dataset
import argparse

def main(args):

    curr = arrow.now()
    print ("Current time : ", curr)

    if(args.basis == "daily") :
        curr = curr.floor('day')
        remove = curr.replace(days=-args.remove)
        add = curr.replace(days=+args.add)
        partitionNameFormat = 'YYYYMMDD'
        timeFormat = 'YYYY-MM-DD 00:00:00'
    elif(args.basis == "monthly"):
        curr = curr.floor('month')
        remove = curr.replace(months=-args.remove)
        add = curr.replace(months=+args.add)
        partitionNameFormat = 'YYYYMMDD';
        timeFormat = 'YYYY-MM-DD 00:00:00'
    else:
        curr = curr.floor('hour')
        remove = curr.replace(hours=-args.remove)
        add = curr.replace(hours=+args.add)
        partitionNameFormat = 'YYYYMMDDHH'
        timeFormat = 'YYYY-MM-DD HH:00:00';

    removeQuery = "alter table %s drop partition p%s" % (args.table, remove.format(partitionNameFormat))
    addQuery = "alter table %s add partition (partition p%s values less than (unix_timestamp('%s')))" % \
           (args.table, add.format(partitionNameFormat), add.format(timeFormat))

    uri = "mysql://" + args.user + ":" + args.password + "@" + args.host + "/" + args.database
    print(uri)

    try:
        if not args.verbose :
            db = dataset.connect(uri)
    except Exception as e:
        print (e)
        return
    try:
        print ("+ Remove a partition")
        print ("\t- Query: ", removeQuery)
        if not args.verbose :
            result = db.query(removeQuery)
        print("\t- Success")
    except Exception as e:
        print("\t- Warn: ", e)

    try:
        print("+ Add a partition")
        print("\t- Query", addQuery)
        if not args.verbose :
            result = db.query(addQuery)
        print ("\t- Success")
    except Exception as e:
        print ("\t- Warn: ", e)

    print ("Complete Work")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("-H", "--host", nargs='?',  default = "127.0.0.1",
                      help = "MySQL database name")
    parser.add_argument("-d", "--database",  default = "aiot",
                      help = "MySQL Server Address")
    parser.add_argument("-t", "--table", default="daily",
                      help = "MySQL Table")
    parser.add_argument("-u", "--user", default="aiot",
                      help = "MySQL User Id")
    parser.add_argument("-p", "--password", default="",
                      help = "MySQL User Password")
    parser.add_argument("-a", "--add", type = int, default=7,
                  help = "a partition to be added ")
    parser.add_argument("-r", "--remove", type = int, default=7,
                  help = "a partition to be removed")
    parser.add_argument("-b", "--basis", default="daily",
                      choices=['daily', 'monthly', 'hourly'], help = "time basis")
    parser.add_argument("-v", "--verbose", default=False, help = "only print query")

    args = parser.parse_args()

    main(args)
