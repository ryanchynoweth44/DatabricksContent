# Databricks Repos 

## Repos and API Overview

With [Databricks Repos](https://docs.databricks.com/repos.html) engineers can version control code at a repository level for easier management and version control. Some really great aspects of Databricks repos are:  
- Cloning repositories 
- Managing branches
- Pushing and pulling changes
- Visually comparing differences upon commit
- Programmatically updating repositories for code deployment via APIs

Check out the documentation available for [best practices for CI/CD Workflows](https://docs.databricks.com/repos.html#best-practices-for-integrating-repos-with-cicd-workflows).  

The [Repos API](https://docs.databricks.com/dev-tools/api/latest/repos.html) is currently in public preview and has the following endpoints:  
1. [Get Repos](https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/get-repos): lists repos available to the user with manage permissions  
1. [Create Repo](https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/create-repo)  
1. [Get a Repo](https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/get-repo): gets a given repository by id  
1. [Update Repo](https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/update-repo): this allows users to update a repository to a given branch or tag.  
    - Need to test if it will pull changes if the branch doesn't change. This isn't an issue because you could create release branches associated to builds.  
    - We will use this in our CI/CD script
1. [Delete Repo](https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/delete-repo)  


### Examples

1. [Programmatically update](./UpdateDatabricksRepoAPI.py) a Databricks Repo via the REST API
  - This is an excellent way to handle deployments within a CICD pipelines 
