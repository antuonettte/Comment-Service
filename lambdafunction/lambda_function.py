import json
import pymysql

# Database configuration
db_host = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
db_user = 'admin'
db_password = 'FrostGaming1!'
db_name = 'comment_db'

# Connect to the database
conn = pymysql.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)
cursor = conn.cursor()


def lambda_handler(event, context):
    # Determine the HTTP method
    http_method = None
    http_method = event['httpMethod']
    
    
    if http_method == 'GET':
        # Handle GET request
        post_id = event['queryStringParameters']['postID']
        return get_comments_by_post(post_id)
    
    elif http_method == 'POST':
        # Handle POST request
        comment_data = json.loads(event['body'])
        return post_comment(comment_data)
    
    else:
        return {
            'statusCode': 400,
            'body': json.dumps(event)
        }
    
def get_comments_by_post(post_id):
    try:
        cursor.execute(f"SELECT * FROM users WHERE post_id = {post_id}")
        comments = cursor.fetchall()

        if comments:
            print(comments)

            return{
                'statusCode' : 200,
                'body' : json.dumps(comments)
            }
        else:
            return {
                'statusCode':404,
                'body': json.dumps('No comments for post ID')
            }
    except Exception as e:
        return {
            'statusCode':500,
            'body' : json.dumps(f'Error: {str(e)}')
        }


def post_comment(comment_data):
    try:
        cursor.execute("INSERT INTO comments (post_id, user_id, content) VALUES (%s, %s, %s)", (comment_data['post_id'], comment_data['user_id'], comment_data['content']))
        conn.commit()
        return {
            'statusCode': 201,
            'body': json.dumps('Comment created successfully')
        }

    except Exception as e:
        conn.rollback()
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }