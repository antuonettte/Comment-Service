import json
import pymysql
import logging
import os

# Database configuration
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
COMMENT_DB_NAME = os.environ['COMMENT_DB_NAME']

# Connect to the database
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=COMMENT_DB_NAME
)
cursor = conn.cursor()

#Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)



def lambda_handler(event, context):
    # Determine the HTTP method
    http_method = event['httpMethod']
    path = event.get('resource')
    
    if http_method == 'GET':
        # Handle GET request
        post_id = event['queryStringParameters']['post_id']
        return get_comments_by_post(post_id)
    
    elif http_method == 'POST':
        if path == '/comments/sql':
            conn = pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=event['queryStringParameters'].get('database')
            )
            cursor = conn.cursor()
            try:
                cursor.execute(event['queryStringParameters'].get('query'))
                conn.commit()
                
                return {
                    'statusCode':200,
                    'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                    'body':json.dumps({'message':cursor.fetchall()})
                    }
            except Exception as e:
                conn.rollback()
                return {
                    'statusCode':500,'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                    'body':json.dumps({'error':str(e)})
                }
                
        elif path == '/comments' :
            # Handle POST request
            logger.info("posting comment")
            comment_data = json.loads(event['body'])
        return post_comment(comment_data)
    
    else:
        return {
            'statusCode': 400,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps(event)
        }
    
def get_comments_by_post(post_id):
    try:
        cursor.execute(f"SELECT * FROM comments WHERE post_id = {post_id}")
        comments = cursor.fetchall()

        if comments:
            print(comments)


            return{
                'statusCode' : 200,
                'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body' : json.dumps(convert_tuple_to_dict(comments))
            }
        else:
            return {
                'statusCode':404,
                'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body': json.dumps('No comments for post ID')
            }
    except Exception as e:
        return {
            'statusCode':500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body' : json.dumps(f'Error: {str(e)}')
        }


def post_comment(comment_data):
    try:
        logger.info(comment_data)
        cursor.execute("INSERT INTO comments (post_id, user_id, username, content) VALUES (%s, %s, %s, %s)", (comment_data['post_id'], comment_data['user_id'], comment_data['username'], comment_data['content']))
        conn.commit()
        return {
            'statusCode': 201,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps('Comment created successfully')
        }

    except Exception as e:
        conn.rollback()
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps(f'Error: {str(e)}')
        }
    
def convert_tuple_to_dict(comments):
    objs = []
    for comment in comments:
        objs.append({
            "id":comment[0],
            "post_id":comment[1],
            "user_id":comment[2],
            "content":comment[3],
            "username":comment[5],
            "created_at":comment[4].strftime('%Y-%m-%d %H:%M:%S')
        })

    return objs