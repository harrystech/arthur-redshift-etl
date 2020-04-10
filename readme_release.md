# Release of ETL code as open-source project

Some files crept into the private repo that had some of our AWS setup hard-coded,
like account number or security groups.
Although these settings should not be useful for folks outside our organization,
we still dropped all files related to AWS settings.
This file outlines the steps taken from our private repo to a public repo.

## References

The following guides were useful:
* [Removing sensitive data from a repository (Github)](https://help.github.com/articles/removing-sensitive-data-from-a-repository/)
* [Completely remove a file from a git repository with git forget-blob](https://ownyourbits.com/2017/01/18/completely-remove-a-file-from-a-git-repository-with-git-forget-blob/)

## Steps

### Discover all files ever used

We can use `git` to find the (first) commit where our account number snuck in.

Let's create a script that has the correct exit code:
```bash
#! /bin/sh
if git grep "ACCOUNT NUMBER"; then
    exit 1
else
    exit 0
fi
```

Then start bi-secting:
```
git bisect start
git bisect good 474fe5
git bisect bad 8a4a9d1
git bisect run ./test.sh
git bisect reset
```

Repeat the above for other settings, like security groups or subnets.
From there, pick up all the files that show up in the bisect log.
Also, test whether these files ever had other names using:
```
git log --name-only --follow --all -- "FILENAME"
```

### Remove old references

Rewriting history changes all objects, so old tags have no meaning:
```
git tag | xargs git tag -d
```

Also, removed all branches other than master.

### Remove files with confidential information

As easy as pie:
```
for FILE in \
    aws_config/daily_consistency_pipeline.json \
    aws_config/data_pipeline.json \
    aws_config/ec2_attributes.json \
    aws_config/ec2_data_pipeline.json \
    aws_config/midday_data_pipeline.json \
    aws_config/pizza_load_pipeline.json \
    aws_config/rebuild_pipeline.json \
    aws_config/refresh_pipeline.json \
    aws_config/validation_pipeline.json \
    bin/aws_ec2.sh \
    ; do

    git filter-branch --force --index-filter \
      "git rm --cached --ignore-unmatch $FILE" \
      --prune-empty --tag-name-filter cat -- --all
done
```

### Cleanup objects

Drop objects that are no longer referenced,
change the reflog such that the rewrites disappear,
pack everything into one object pack.
```
git reflog expire --expire-unreachable=now --all
git repack -A -d
git gc --aggressive --prune=now
```
