# Port Branch Changes from stage-jdk8 (Java 8) to stage (Java 21)

Port changes from the source branch (based on `stage-jdk8`) to a new branch based on `stage`.

## Instructions

The user will provide a source branch name as an argument. Use it to perform the port.

## Target Branch Naming

The target branch name MUST be derived from the source branch name by removing the `_jdk8` suffix:
- Source: `CLIENT-1234_feature_jdk8` -> Target: `CLIENT-1234_feature`
- If the source branch doesn't have a `_jdk8` suffix, append `_jdk21` to create the target name:
  - Source: `bugfix-something` -> Target: `bugfix-something_jdk21`

## Steps

1. **Validate** the source branch exists and is based on `stage-jdk8`.
2. **Analyze** the commits on the source branch that are not on `stage-jdk8`:
   ```
   git log --oneline stage-jdk8..<source-branch>
   ```
3. **Create** the target branch off `stage`:
   ```
   git checkout stage
   git pull
   git checkout -b <target-branch-name>
   ```
4. **Cherry-pick** the commits from the source branch onto the target branch. If there are conflicts, resolve them with the Java 21 adaptations listed below.
5. **Adapt** the code for Java 21 idioms (see adaptation rules below).
6. **Build** to verify:
   ```
   mvn clean install
   ```

## Java 8 to Java 21 Adaptation Rules

When porting code, apply these modernization transformations where appropriate:

### Language Features
- **Traditional instanceof + cast** -> Use pattern matching for instanceof
  ```java
  // Java 8
  if (obj instanceof String) { String s = (String) obj; use(s); }
  // Java 21
  if (obj instanceof String s) { use(s); }
  ```
- **Verbose switch statements** -> Consider switch expressions where cleaner
  ```java
  // Java 8
  String result;
  switch (x) {
      case 1: result = "one"; break;
      case 2: result = "two"; break;
      default: result = "other"; break;
  }
  // Java 21
  var result = switch (x) {
      case 1 -> "one";
      case 2 -> "two";
      default -> "other";
  };
  ```
- **String concatenation for multiline** -> Consider text blocks where appropriate
- **Explicit types for local variables** -> Use `var` where the type is obvious from context

### API Differences
- **`Collections.unmodifiableList(Arrays.asList(...))`** -> Use `List.of(...)`
- **`Collections.unmodifiableSet(new HashSet<>(Arrays.asList(...)))`** -> Use `Set.of(...)`
- **`string.trim().isEmpty()`** -> Use `string.isBlank()`
- **`!optional.isPresent()`** -> Use `optional.isEmpty()`
- **`.collect(Collectors.toList())`** -> Use `.toList()`
- **Traditional I/O patterns** -> Consider `Files.readString()` / `Files.writeString()` where appropriate

### Project-Specific Differences
- The `stage` branch uses `aerospike-client-jdk21` artifact instead of `aerospike-client-jdk8`
- pom.xml uses `java.version=21` instead of `java.version=1.8` / `java.api=8`
- The `stage` branch may have additional methods (e.g., `estimateKeySize()`) not present in jdk8
- Do NOT modify pom.xml java version settings - those are already correct on `stage`

### Important
- **Be conservative with modernization**: Only modernize code that is part of the change being ported. Do not refactor surrounding unchanged code.
- Preserve the original commit messages when cherry-picking
- If a file does not exist on `stage`, check if the functionality lives in a different file or should be skipped
- After all changes, run `mvn clean install` and fix any compilation errors
