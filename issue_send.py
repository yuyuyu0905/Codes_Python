import json
import requests
import traceback
from config.settings import *
#Config the user name and password

USERNAME = github_username
PASSWORD = github_token

#Add issues to
REPO_OWNER = github_username
REPO_NAME = github_repository


def test_function(test):
    try:
        a = []
        b = a[2]
    except Exception, e:
        print Exception, repr(e)
        print str(traceback.format_exc())
        issue_send('exception_on_test', body=traceback.format_exc())


def issue_send(title, body=None, assignee=None, milestone=None, labels=None):

    if not labels:
        labels = []
    url = 'https://api.github.com/repos/%s/%s/issues' % (REPO_OWNER, REPO_NAME)
    print url
    session = requests.session()
    session.auth = (USERNAME, PASSWORD)
    issue = {
        'title': title,
        'body': body,
        'assignee': assignee,
        'milestone': milestone,
        'labels': labels
    }

    r = session.post(url, json.dumps(issue))
    print r, r.content

test_function(2)
