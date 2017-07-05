import json, math, sys, time

userlist = {} #Master dictionary, containing all user ids
timeindex = 0 # Because the only way to determine a later timestamp from two identical 
              # timestamps is by finding which one came first, an index of timestamps is kept

#Add to user to userlist if user doesn't exist. Each user element has a set for friends, and a list for timestamp-timeindex/purchase lists
def create_uid(uid):
    if uid not in userlist:              
        userlist.update({uid:(set(),[])})

#Add or discard (remove only if they exist) friends from the user element's friend set
def change_friend(uid1,uid2,befriend):
    if befriend:
        userlist[uid1][0].add(uid2)
        userlist[uid2][0].add(uid1)
    else:
        userlist[uid1][0].discard(uid2)
        userlist[uid2][0].discard(uid1)

#Append timestamp-timeindex/purchase list to the user element's timestamp/purchase list. Future sorting will go by the timestamp-timeindex tuple: timestamp (in integer form for efficiency) sorted primarily, and the timeindex sorted secondarily
def update_purchases(uid,timestamp,amount,track):
    global timeindex

    userlist[uid][1].append([(int(time.mktime(time.strptime(timestamp,"%Y-%m-%d %H:%M:%S"))),timeindex),float(amount)])
    timeindex += 1

    #if there are more than the desired tracked purchases in the social network, remove any extra earlier timestamps
    if len(userlist[uid][1]) > track:
        userlist[uid][1].sort()
        userlist[uid][1].pop(0)
    
def main(*argv):

    y = open(argv[1],'w') #flagged_purchases are written here
    files = [argv[2],argv[3]] #batch_log and stream_log are specified here. Batch log first!

    for f in range(len(files)): #f=0: batch_log, f=1: stream_log
        x = open(files[f],'r')

        for i in x:
            try:
                j = json.loads(i) #Read in json strings from file. Include try/except in case of trailing endline characters or errata
            except:
                continue

            if 'D' in j and 'T' in j: 
                degrees = int(j['D']) #Determine the desired number of tracked purchases in the social network
                tracked = int(j['T']) #Determine how many degrees should be considered for a social network
                continue

            if j['event_type'] == 'befriend': 

                create_uid(j['id1']) #Call user-defined create_uid function to add user to master dictionary, if the user is not already in
                create_uid(j['id2'])
                change_friend(j['id1'],j['id2'],1) #add friend function (third argument set to one for befriending)

            if j['event_type'] == 'unfriend':
                
                create_uid(j['id1'])
                create_uid(j['id2'])
                change_friend(j['id1'],j['id2'],0) #remove friend function (third argument set to zero for unfriending)

            if j['event_type'] == 'purchase': 

                create_uid(j['id'])
                update_purchases(j['id'],j['timestamp'],j['amount'],tracked) #call user-defined update_purchases to add current purchase 

                if f: #if we are on the second file, the stream_log file, then we need to assess whether this is an anomalous purchase
                    purchase_list = []
                    friend_list = [j['id']]
                    degrees_lists = [[j['id']]]
                    my_stddev_sigma = 0

                    for a in range(degrees):         #Number of degrees determine the depth of the social network
                        degrees_lists.append([])     #First include the friends of the current user
                        for b in degrees_lists[a]:          #Next, if degrees > 1, determine the friends of the friends of the current user
                            for c in list(userlist[b][0]):
                                if c not in friend_list:
                                    degrees_lists[a+1].append(c)
                                    friend_list.append(c)

                    friend_list.remove(j['id'])   #Do not include the current user in the current user's social network

                    for a in friend_list:
                        purchase_list += userlist[a][1]  #Find out the timestamps/purchases of the users in the social network

                    if len(purchase_list) > 1: #One purchase is not big enough for our calculations...
                        if len(purchase_list) > tracked:  #We only want the most recent, desired tracked purchases from the social network
                            purchase_list.sort()
                            purchase_list = purchase_list[-1*tracked:]

                        my_mean = sum([k[1] for k in purchase_list]) / float(len(purchase_list)) #Calculate the mean

                        for k in purchase_list:
                            my_stddev_sigma += (k[1]-my_mean)*(k[1]-my_mean) #Calculate the sum for the standard deviation
        
                        my_stddev = math.sqrt(my_stddev_sigma / float(len(purchase_list))) #Find the square root of the quotient for the std dev
                        
                        if float(j['amount']) > (3*my_stddev+my_mean): #If the purchase is 3 standard deviations from the mean, flag it
                            y.write("{\"event_type\":\"purchase\", \"timestamp\":\"%s\", \"id\":\"%s\", \"amount\":\"%s\", \"mean\":\"%.2f\", \"sd\":\"%.2f\"}\n" % (j['timestamp'],j['id'],j['amount'],my_mean,my_stddev))

        x.close() #Close the log file
    y.close() #Close the flagged purchases file

if __name__=="__main__":
    main(*sys.argv)
