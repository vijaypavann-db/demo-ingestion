import os


def update_repo():
    """ Access Secrets using `Databricks SDK`
    """
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()

    path = "/Repos/vijay.pavan@databricks.com"
    repos_list = ws.repos.list(path_prefix = path)

    # id=3223104499255692 demo_ingestion
    [print(repo, repo.id) for repo in repos_list if repo.branch == "main"]  

    ws.repos.update(repo_id=3223104499255692)
    
update_repo()