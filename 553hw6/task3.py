import random
import tweepy
from sys import argv
port = argv[1]
output_file = argv[2]
# Step 1: Creating a StreamListener
# override tweepy.StreamListener to add logic to on_status


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        global sequence_number
        global tweets_tags
        global tags_dict
        global file
        tags = status.entities.get("hashtags")
        if tags:
            sequence_number += 1
            # sample size is 100
            if sequence_number <= 100:
                tweets_tags.append([tag['text'] for tag in tags])
            else:
                # Keep the n th tweet with the probability of 100 / n.
                # If keep the n th tweet, discard one tweet in sample randomly
                if random.randint(1, sequence_number) in range(1, 101):
                    discard_ind = random.randint(0, 99)
                    tweets_tags.pop(discard_ind)
                    tweets_tags.append([tag['text'] for tag in tags])
                for tags in tweets_tags:
                    for tag in tags:
                        if tag in tags_dict.keys():
                            tags_dict[tag] += 1
                        else:
                            tags_dict[tag] = 1
                # sort by frequency and keep top 3
                sorted_tag_freq = sorted(tags_dict.items(), key=lambda x: (-x[1], x[0]))
                freq_output = []
                count = 0
                for i in range(0, 7):
                    if count > 2:
                        break
                    if sorted_tag_freq[i][1] >= sorted_tag_freq[i + 1][1]:
                        freq_output.append(sorted_tag_freq[i])
                        if sorted_tag_freq[i][1] > sorted_tag_freq[i + 1][1]:
                            count += 1
                file.write("The number of tweets with tags from the begining: " + str(sequence_number) + "\n")
                for tag_freq in freq_output:
                    file.write(tag_freq[0] + " : " + str(tag_freq[1]) + "\n")
                file.write("\n")
                file.flush()

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False
file = open(output_file, 'w+')
api_key = "ZhD7taucWEsp2bj0IY4uEz9BG"
api_secret_key = "94wVTreqvhIrVQT8ymkc2wSnFCSEHry9azpuH2Zkv5gKMgfDvo"
access_token = "1255723557369245698-6mFJHnU7MmtBGWVCmuKS90NnQChe5U"
access_token_secret = "YTqPXeVKzFUJfyxDA5bGKi7owrXri2THmuLeFn03DSaR9"
auth = tweepy.OAuthHandler(consumer_key=api_key, consumer_secret=api_secret_key)
auth.set_access_token(key=access_token, secret=access_token_secret)
api = tweepy.API(auth)
sequence_number = 0
tweets_tags = []
tags_dict = {}
# Step 2: Creating a Stream
myStream = tweepy.Stream(auth=api.auth, listener=MyStreamListener())
# Step3: Starting a Stream
myStream.filter(track=['covid'])
