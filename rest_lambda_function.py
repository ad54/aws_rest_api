"""This is the lambda function for rest api , which will provide data from dynamo db.
You need to pass the name of the sport and team. It will provide recent reords of the team.
If the searched sport is not availble, it will list all the available, the same for the searched team. 
"""
import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
# connection to dynamo table
dynamodb = boto3.resource('dynamodb')
# mention the table name
table_1 = dynamodb.Table()

def get_sports():
    # get all the sports available
    get_sports_data = table_1.scan()
    sports_data = set()
    for item in get_sports_data['Items']:
        sports_data.add(item.get('sport'))
    msg = f"please enter one of sports : {','.join(sports_data)}"
    return msg


def get_teams(sport):
    # get the teams of perticular sport
    get_teams_data = table_1.scan(FilterExpression=Attr('sport').eq(str(sport)))
    if get_teams_data['Items']:
        teams = set()
        for item in get_teams_data['Items']:
            teams.add(item.get('team'))
        msg = f"please enter one of teams for sport : {sport} : {','.join(teams)}"
    else:
        # if no teams found it will call get sports function :this scenario is like invalid team search
        msg = get_sports()
    return msg


def get_data(sport='', team=''):
    table_1 = dynamodb.Table('sportsData')
    if not sport:
        msg = ("please eneter sports")
        return {"msg": msg}
    # if team is not given it will list all the teams for that sport
    if sport and (not (team)):
        msg = get_teams(sport)
        return {"msg": msg}

    response1 = table_1.scan(FilterExpression=Attr('sport').eq(str(sport)) & Attr('team').eq(str(team)))
    if not response1['Items']:
        msg = get_teams(sport)
        return {"msg": msg}
    else:
        # on successfull request list results and generate output
        msg_list = list()
        for item in response1['Items']:
            # print(item)
            if (int(item.get('team_score', ''))) > (int(item.get('op_team_score', ''))):
                status = 'beat'
            else:
                status = 'lose'
            msg = f"{item.get('team')} {status} {item.get('op_team', '')} {item.get('team_score', '')}-{item.get('op_team_score', '')} on {item.get('date', '')}"
            msg_list.append(msg)
        return {"msg": (msg_list)}


def lambda_handler(event, context):
    # get paramaters from the request
    sport = event['queryStringParameters'].get('sport','')
    team = event['queryStringParameters'].get('team','')
    
    # call get data function
    messgae = get_data(sport=sport, team=team)
    
    # Construct http response object
    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['body'] = json.dumps(messgae)

    return responseObject
