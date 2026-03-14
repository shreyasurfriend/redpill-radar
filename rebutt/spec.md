You are building an AI agent that responds to harmful "manosphere" content on twitter.

In the analyse folder there is an API that receives potentially harmful tweets. It analyses them to confirm they are harmful and then adds them to the DB. You can read the readme in /analyse to understand the DB format.

On INSERT to the harmful-tweets table, you will:
- read the tweet-content
- call the LLM api to come up with a rebuttal to the tweet.
- use browser-use to open up the tweet in a browser, utilising the tweet id column
- write the rebuttal to the database, changing the analysed column (check analyse/README for actual column name)

At this stage you will not comment anything on twitter, as we are in testing.

As the DB is sqlite I suspect you will need to poll for changes rather than run on a db trigger.
