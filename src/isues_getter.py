import json
import logging
from pathlib import Path
from typing import List, Dict

import click
from tqdm import tqdm
from github import Github
from github.Issue import Issue

logger = logging.getLogger("__name__")


def _get_issues_data(issue: Issue) -> dict:

    comments = issue.get_comments()
    issue_data = {
        "body_len": len(issue.body) if issue.body is not None else "None",
        "creation_date": str(issue.created_at),
        "labels": [x.name for x in issue.labels],
        "id": issue.id,
        "title_len": len(issue.title) if issue.title is not None else "None",
        "last_modified_date": issue.last_modified,
        "author": issue.user.name,
        "comments": [{"id": comment.id,
                      "comment_len": len(comment.body),
                      "creation_date": str(comment.created_at),
                      "author": comment.user.name,
                      "update_date": str(comment.updated_at)}
                     for comment in comments],
    }

    return issue_data


@click.argument("access_token", type=str)
@click.argument("repo_name", type=str)
@click.argument("log_path", type=Path)
def _get_issues_info(access_token: str,
                     repo_name: str,
                     log_path: Path)\
        -> List[Dict]:
    """
    Gets statistics for issues combined in convenient way for
    process mining
    :param access_token: token of your Github account to get access for api
    :param repo_name: name of parsed repo in format "author/repo_name"
    :log_path: path where to store json object of issues
    :return:
    """
    user = Github(access_token, per_page=100)
    repo = user.get_repo(repo_name)

    issues_obj = []

    issues = repo.get_issues()
    for issue in tqdm(issues, total=issues.totalCount):
        issues_obj.append(_get_issues_data(issue))

    with open(log_path, "w") as fp:
        json.dump(issues_obj, fp)

    return issues_obj


get_issues_info = click.command()(_get_issues_info)

if __name__ == "__main__":
    get_issues_info()
