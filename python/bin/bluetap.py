import os
from atproto import Client
import socket
import json
from time import sleep

# Credits https://github.com/sspaeti/bsky-atproto/blob/main/python/streaming_into_motherduckdb.py

def fetchPost():
    global bsky_client
    global hashtag
    global s
    global fluentd_host
    global fluentd_port
    global postcache
    cursor = None
    while True:
        print (f"Fetching posts with hashtag {hashtag}")
        fetched = bsky_client.app.bsky.feed.search_posts(
                        params={'q': f'#{hashtag}', 'cursor': cursor, 'limit': 25}
                    )
        for post in fetched.posts: 
            if post.uri not in postcache:
                    postcache.add(post.uri)
            else:
                print(f"Skipping already processed post {post.uri}")
                continue
            # Safely handle langs attribute
            langs = None
            if hasattr(post.record, 'langs') and post.record.langs:
                try:
                    langs = ','.join(post.record.langs)
                except:
                    langs = None
            post_data = {
                        'uri': post.uri,
                        'cid': post.cid,
                        'author': post.author.handle,
                        'text': post.record.text,
                        'created_at': post.record.created_at,
                        'hashtag': hashtag,
                        'langs': langs
                    }
            
            match logger:
                case "stdout":
                    print(post_data)
                case "fluentd_tcp":
                    try:
                        bytes=s.send(json.dumps(post_data).encode('utf-8'))
                        print(f"Sent post with {bytes} bytes to Fluentd")
                        s.send(b'\n')
                    except Exception as e:
                        print(f"Error sending to Fluentd: {e}")
    
        if not fetched.cursor:
            return            
        cursor = fetched.cursor
        print(f"Next cursor: {cursor}")

def initializeLogger():
    global s
    match logger:
        case "stdout":
            print("Logger: stdout")
        case "fluentd_tcp": 
            print(f"Logger: fluentd_tcp at {fluentd_host}:{fluentd_port}")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((fluentd_host, fluentd_port))
            except Exception as e:
                print(f"Error connecting to Fluentd: {e}")
                # TODO: Handle error

def finalizeLogger():
    global s
    match logger:
        case "fluentd_tcp":
            s.close()
            print("Closed connection to Fluentd")

print("*** BlueSky connector v 1.0 ***")

app_password = os.getenv("BLUESKY_APP_PASSWORD") # TODO: Move to secret
handle = os.getenv("BLUESKY_HANDLE")
logger = os.getenv("BLUESKY_LOGGER", "stdout")


# Define Fluentd address and port
fluentd_host = 'fluentd'
fluentd_port = 5000

# Create AT Proto Client
bsky_client = Client()

password = app_password
bsky_client.login(handle, password)
hashtag = "tapunict" # TODO move into env variable
postcache = set() # Cache for processed posts (can be improved using timestamp or firehose API or feed API)

while True:
    print("Starting new search")
    initializeLogger()
    fetchPost()
    finalizeLogger()
    sleep(5)

