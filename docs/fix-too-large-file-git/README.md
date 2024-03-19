# What to do when you tried to push too large of a file to Github

 If on the off chance you are reading this because you did not create a `.gitignore` like we asked :eyes: ​and already committed and attempted to push a large file, you are likely seeing an error like so: 

```
remote: error: Trace: aa23e631c36738d472217b010926247acfeb9fd72b7ad295ab2d596573770574
remote: error: See https://gh.io/lfs for more information.
remote: error: File hdma-wi-2021.csv is 166.84 MB; this exceeds GitHub's file size limit of 100.00 MB
remote: error: GH001: Large files detected. You may want to try Git Large File Storage - https://git-lfs.github.com.
To github.com:USERNAME/sample.git
 ! [remote rejected] main -> main (pre-receive hook declined)
error: failed to push some refs to 'github.com:USERNAME/sample.git'
```

Thankfully, this is a common problem (I've done it way too many times). To fix it, please follow these steps:

1) Locally delete the large file and commit that delete
2) Soft reset back X commits to before you pushed the large file using: `git reset --soft HEAD~X`; note that you will need to determine how many commits X should be.  
3) Stage your current changes using `git add` (if you don't have any, for the sake of the fix just add a comment in a file so you can commit) and then commit and push. 

