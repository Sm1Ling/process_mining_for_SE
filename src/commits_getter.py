import logging

import click
import pandas as pd
from tqdm import tqdm
from github import Github
from github.Commit import Commit

logger = logging.getLogger("__name__")


def _get_commit_data(commit: Commit) -> dict:

    commit_data = {
        "author": commit.commit.author.name,
        "date": commit.commit.last_modified,
        "sha": commit.commit.sha,
        "parent": [c.sha for c in commit.commit.parents],
        "files_num": len(commit.files),
        "additions": commit.stats.additions,
        "deletions": commit.stats.deletions
    }
    return commit_data


@click.argument("access_token", type=str)
@click.argument("repo_name", type=str)
@click.argument("log_path", type=str)
def _get_commits_info(access_token: str, repo_name: str, log_path: str):
    """
    Gets statistics for commits combined in convenient way for
    process mining
    :param access_token: token of your Github account to get access for api
    :param repo_name: name of parsed repo in format "author/repo_name"
    :return:
    """
    user = Github(access_token, per_page=100)
    repo = user.get_repo(repo_name)

    branches = repo.get_branches()
    commits_sha = set()
    commits_obj = []

    for i, branch in enumerate(branches):
        commits = repo.get_commits(sha=branch.name)
        for commit in tqdm(commits, total=commits.totalCount, desc=branch.name):
            if commit.commit is None:  # some strange commits
                logger.warning(f"\nCommit {commit.sha} is None")
                continue
            if commit.sha in commits_sha:  # reached shared branch
                logger.warning(f"\nCommit {commit.sha} already has been written")
                break
            commits_sha.add(commit.sha)
            commits_obj.append(_get_commit_data(commit))
        df = pd.DataFrame(commits_obj)
        df.to_csv(f"{log_path}{i}_{branch.name}.csv")

    return commits_obj


get_commits_info = click.command()(_get_commits_info)

if __name__ == "__main__":
    get_commits_info()
