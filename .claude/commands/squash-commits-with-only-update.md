---
allowed-tools: Bash(git add:*), Bash(git status:*), Bash(git commit:*)
description: Create a git commit
---

## Context

- Current git status: !`git status`
- Current git diff (staged and unstaged changes): !`git diff HEAD`
- Current branch: !`git branch --show-current`
- Recent commits: !`git log -40`

## Your task

Based on the above changes, squash the commits with 'update' to the first commit with not only 'update'. The new commit message is the commit with not only 'update'.
Ask user to confirm before pushing the current branch to the remote branch with the same name on origin.
