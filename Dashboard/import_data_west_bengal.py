import json
import numpy as np
import pandas as pd
import os
import glob
import random
import tweepy
n=143
access_token = "813274164492529665-wPAJPScMbuU8oOKlvoHO3m4kK14pL0z"
access_token_secret = "jMD3lboNidIti6RoygcLH32OrTem2uddgWKE8ZuHY0AMv"
consumer_key = "dvgZ3nJuemHAV7gY7v2IPpPEZ"
consumer_secret = "LL8E0WYTg8iKh12IKbCAeQ63xyZsi48GXDq9GLb7AoaCAgxGpb"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

def west_bengal():
    df_list=[]
    name_updated=[]
    path='/home/avigupta/Database/covid_india_tweets_data/data_public/States/'
    json_pattern = os.path.join(path,'West Bengal/*.json')
    file_list = glob.glob(json_pattern)
    file_list.sort()
    names = [os.path.basename(x) for x in file_list]
    for i in range(len(names)):
        updated=os.path.splitext(names[i])[0]
        name_updated.append(updated)
    for file in file_list:
        data=pd.read_json(file)
        df_list.append(data)
    for i in range(len(df_list)):
        df_list[i].columns=['tweetid']
        df_list[i]['Date']=name_updated[i]
        df_list[i]['State']='West Bengal'
        df_list[i] = df_list[i].sample(n)
    df_West_Bengal = pd.concat(df_list)
    df_West_Bengal = df_West_Bengal.reset_index()
    return df_West_Bengal

if __name__=='__main__':
    df_West_Bengal = west_bengal()
    del df_West_Bengal["index"]
    output = []
    for i,j in df_West_Bengal.iterrows():
        try:
            tweets_list = api.get_status(id=df_West_Bengal['tweetid'][i])
            tweet_id = tweets_list.id
            created_at = tweets_list.created_at
            text = tweets_list.text
            location = tweets_list.user.location
            is_quote_status = tweets_list.is_quote_status
            retweet_count = tweets_list.retweet_count
            line = {'id':tweet_id, 'created_at' : created_at,'text':text, 'location':location,'quote_status':is_quote_status, 'retweet_count' : retweet_count}
            output.append(line)
            print(i, end="  ")
        except:
            continue
    df=pd.DataFrame(output)
    df['state'] = df_West_Bengal['State']
    df.to_csv('output_West_Bengal.csv')
    print("finished with \n",df.count(),"\ndata points")