(**

# How to contribute #

This is how you can make a "pull request", to suggest your modifications to be accepted to the code base. 

### 1. Download or clone the repository to your computer. ###

   (Or take `git pull` if you already have the repository.)

   ![Download](https://imgur.com/FFSDb0L.png)

   You need to get the .git folder inside the repository. 
   If the download doesn't work, you can use clone, which creates a separate folder and downloads the repository:

```
   git clone https://github.com/fsprojects/SQLProvider.git
```
   
### 2. Edit and Commit ###

   Here are some [tech details](techdetails.html).

   Do the modifications and check the build and tests are working. Commit the changes to your local repository.


### 3. Fork the GitHub repository. ###

   This will "copy" the repository for your account.

   ![Fork](https://imgur.com/cMOSS1R.png)

   
### 4. Get Your Url ###

   In GitHub, go to **your** copy of the repository (under your profile, Repositories-tab) and under Download, get the repository URL, e.g. https://github.com/myname/SqlProvider.git


### 5. Add a Remote ###

   With command line, add a remote to your repository URL: 

```
   git remote add myrepo https://github.com/myname/SqlProvider.git
```

   Use the name you want for the repository and note your GitHub account in the URL. You can check the remotes with `git remote -v`.

   
### 6. Push to Your Remote ###

   Push the latest version to your repository with 
   `git push myrepo`. You should see the modifications in GitHub under your repository.


### 7. Create a Pull Request ###

   In GitHub, under **your** repository, press the Create pull request -button. By default, everything should be correct: The base-fork is the one to which you want to send the modifications, and the head fork is your fork, so follow the wizard.

   ![PullRequest](https://imgur.com/CD525AG.png)

   
### 8. Done. ###

   Your pull request should be visible under the "Pull requests" -tab in the original repository. When you do more commits, you can ignore parts 3, 4 and 5, they has to be done just once.

   ![The process](https://i.imgur.com/BrngItg.png)
   
*)
