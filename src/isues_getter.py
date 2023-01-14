import json
import logging
from pathlib import Path
from typing import Dict, List

import click
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
        "closed_at": str(issue.closed_at),
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
@click.argument("log_table_path", type=Path)
def _get_issues_info(access_token: str,
                     repo_name: str,
                     log_path: Path,
                     log_table_path: Path) \
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
    total = 0
    issues = repo.get_issues(state='closed')
    try:
        for issue in issues:
            total += 1
            issues_obj.append(_get_issues_data(issue))
    except:
        print("stopped at " + str(total) + " issues")
    print(total)
    _write_issue_objects_to_table(issues_obj, log_table_path)
    with open(log_path, "w") as fp:
        json.dump(issues_obj, fp)

    return issues_obj


def _write_issue_objects_to_table(issues_obj: list, path: Path):
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(
            '"body_len", "creation_date", "labels", "id", "title_len", "last_modified_date", "closed_at", "author", "comments_num"')
        for issue in issues_obj:
            if len(issue["labels"]) == 0:
                fp.write("\n")
                fp.write(", ".join([str(issue["body_len"]),
                                    issue["creation_date"],
                                    "",
                                    str(issue["id"]),
                                    str(issue["title_len"]),
                                    str(issue["last_modified_date"]),
                                    str(issue["closed_at"]),
                                    str(issue["author"]).replace(",", ";"),
                                    str(len(issue["comments"]))]))
            else:
                for label in issue["labels"]:
                    issue_row = ", ".join([str(issue["body_len"]),
                                           issue["creation_date"],
                                           str(label).replace(",", ";"),
                                           str(issue["id"]),
                                           str(issue["title_len"]),
                                           str(issue["last_modified_date"]),
                                           str(issue["closed_at"]),
                                           str(issue["author"]).replace(",", ";"),
                                           str(len(issue["comments"]))])
                fp.write("\n")
                fp.write(issue_row)


get_issues_info = click.command()(_get_issues_info)

if __name__ == "__main__":
    get_issues_info()
    # with open("../resources/total_issues.json", "r") as fp:
    #     res = json.loads(fp.read())
    # _write_issue_objects_to_table(res, Path("table.csv"))
