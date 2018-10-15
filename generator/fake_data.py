from faker import Faker
import random
import boto3
import os
import sys
import datetime


bucket="<Your Bucket>"
prefix="<Prefix>"

def upload_to_s3(type,file_name,file_path):
    try:
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(file_path,bucket,'{}/{}/{}'.format(prefix,type,file_name))
        waiter = s3.meta.client.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key='{}/{}/{}'.format(prefix,type,file_name))
        os.remove(file_path)
    except Exception as e:
        print e
        raise(e)

def create_file(type,file_counter):
  f_ptr = open("data/{}.{}.csv".format(type,file_counter),"a")
  return f_ptr

def should_create_new(file_path):
    print "size of {} : {}".format(file_path,os.path.getsize(file_path)/(1024*1024))
    return  (os.path.getsize(file_path)/(1024*1024) > 256)

def wake_up(type,f_ptr,file_counter):
    print "entering wakeup"
    if (should_create_new("data/{}.{}.csv".format(type,file_counter)) == True):
        f_ptr.close()
        upload_to_s3(type,"{}.{}.csv".format(type,file_counter),"data/{}.{}.csv".format(type,file_counter))
        file_counter = file_counter+1
        f_ptr = create_file(type,file_counter)
    return f_ptr, file_counter


def main(argv):

    fake=Faker()
    number_of_custs=30000000
    cust_num=1000
    account_id=10000
    tran_id=100
    num_accounts=0
    counter=0
    cust_f_counter=1
    acct_f_counter=1
    tran_f_counter=1

    cust_f = create_file("customer",cust_f_counter)
    acct_f = create_file("account",acct_f_counter)
    tran_f = create_file("transactions",tran_f_counter)

    try:
        #Customer
        while counter < number_of_custs:
            #print("{},{},{}".format(cust_num,fake.first_name(),fake.last_name()))
            cust_f.write("{},{},{}\n".format(cust_num,fake.first_name(),fake.last_name()))

            acct_random=random.randrange(1,3)
            #Account
            num_accounts=0
            while num_accounts < acct_random:
                acct_open_date = fake.date_time_between(start_date="-30y", end_date="now", tzinfo=None)
                balance=random.uniform(1000.10, 1000000.00)
                #print("{},{},{},{:.2f},{}".format(account_id,fake.uuid4(),acct_open_date,balance,cust_num))
                acct_f.write("{},{},{},{:.2f},{}\n".format(account_id,fake.uuid4(),acct_open_date,balance,cust_num))

                #Transactions
                tran_random=random.randrange(22,30)
                num_tran=0
                tran_date=acct_open_date
                while num_tran < tran_random:
                    tran_date=fake.date_time_between(start_date=tran_date,end_date="now",tzinfo=None)
                    tran_amt=random.uniform(10, balance/10)
                    #print("{},{},{},{},{:.2f}".format(tran_id,account_id,random.choice(['D','C','L','S']),tran_date,tran_amt))
                    tran_f.write("{},{},{},{},{:.2f}\n".format(tran_id,account_id,random.choice(['D','C','L','S']),tran_date,tran_amt))
                    num_tran=num_tran+1
                    tran_id=tran_id+1
                #End Transaction Loop

                account_id=account_id+1
                num_accounts=num_accounts+1
            #End Account Loop
            if ( datetime.datetime.now().minute%5 == 0 & datetime.datetime.now().second%60 == 0):
                cust_f,cust_f_counter = wake_up("customer",cust_f,cust_f_counter)
                acct_f,acct_f_counter = wake_up("account",acct_f,acct_f_counter)
                tran_f,tran_f_counter = wake_up("transactions",tran_f,tran_f_counter)

            cust_num=cust_num+1
            counter=counter+1
        #End Customer Loop

    except Exception as e:
        print("Error happened : {}".format(e))
    finally:
        cust_f.close()
        acct_f.close()
        tran_f.close()

if __name__ == "__main__":
    main(sys.argv)
