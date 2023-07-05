import os


def get_secret_sdk():
    """ Access Secrets using `Databricks SDK`
    """
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()

    scopes = ws.secrets.list_scopes()
    print(scopes, type(scopes))
    # repos_list = list(ws.repos.get())
    # # [print(repo) for repo in repos_list]
    print(f"len(scopes):: {len(scopes)} ")

get_secret_sdk()