Client Development Guide
========================

AI-Assisted Branch Porting
--------------------------

This repository maintains two main staging branches:

* **stage** — targets Java 21
* **stage-jdk8** — targets Java 8

Custom skills are provided for [Claude Code](https://claude.ai/claude-code) and
[Cursor](https://cursor.com) to automate porting changes between these branches,
including Java version-specific code adaptations.

### Claude Code

Use the slash commands in the Claude Code CLI or IDE extension:

    /port-to-jdk8 <source-branch>

Ports commits from a `stage`-based branch to a new branch based on `stage-jdk8`.
The target branch is named `<source-branch>_jdk8`.

    /port-to-jdk21 <source-branch>

Ports commits from a `stage-jdk8`-based branch to a new branch based on `stage`.
The target branch is named by removing the `_jdk8` suffix (or appending `_jdk21`
if no suffix is present).

### Cursor

Open the Cursor chat or agent panel and type:

    /port-to-jdk8

or

    /port-to-jdk21

then provide the source branch name when prompted.

### What the skills do

1. Cherry-pick commits from the source branch onto the correct base.
2. Adapt Java language features (records, switch expressions, pattern matching,
   `var`, text blocks) and API calls (`List.of()` vs `Arrays.asList()`,
   `isBlank()` vs `trim().isEmpty()`, etc.) for the target Java version.
3. Handle project-specific differences (artifact names, pom.xml settings,
   methods that only exist on one branch).
4. Run `mvn clean install` to verify the build.
