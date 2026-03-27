---
name: port-to-jdk8
description: Port changes from a stage-based branch to a stage-jdk8-based branch, adapting Java 21 code to Java 8 compatibility
argument-hint: [source-branch]
allowed-tools: Bash, Read, Edit, Write, Grep, Glob
user-invocable: true
---

# Port Branch Changes from stage (Java 21) to stage-jdk8 (Java 8)

Port changes from the source branch `$ARGUMENTS` (based on `stage`) to a new branch based on `stage-jdk8`.

## Target Branch Naming

The target branch name MUST be derived from the source branch name by appending `_jdk8`:
- Source: `CLIENT-1234_feature` -> Target: `CLIENT-1234_feature_jdk8`
- Source: `bugfix-something` -> Target: `bugfix-something_jdk8`

## Steps

1. **Validate** the source branch `$ARGUMENTS` exists and is based on `stage`.
2. **Analyze** the commits on the source branch that are not on `stage`:
   ```
   git log --oneline stage..$ARGUMENTS
   ```
3. **Create** the target branch off `stage-jdk8`:
   ```
   git checkout stage-jdk8
   git pull
   git checkout -b <target-branch-name>
   ```
4. **Cherry-pick** the commits from the source branch onto the target branch. If there are conflicts, resolve them with the Java 8 adaptations listed below.
5. **Adapt** the code for Java 8 compatibility (see adaptation rules below).
6. **Build** to verify:
   ```
   mvn clean install
   ```

## Java 21 to Java 8 Adaptation Rules

When porting code, apply these transformations:

### Language Features
- **Records** -> Replace with traditional classes (private final fields, constructor, getters, equals/hashCode/toString)
- **Sealed classes/interfaces** -> Remove `sealed`, `permits`, `non-sealed` keywords; use regular class hierarchy
- **Pattern matching for instanceof** -> Use traditional instanceof + explicit cast
  ```java
  // Java 21
  if (obj instanceof String s) { use(s); }
  // Java 8
  if (obj instanceof String) { String s = (String) obj; use(s); }
  ```
- **Switch expressions** -> Convert to switch statements or if-else chains
  ```java
  // Java 21
  var result = switch (x) {
      case 1 -> "one";
      case 2 -> "two";
      default -> "other";
  };
  // Java 8
  String result;
  switch (x) {
      case 1: result = "one"; break;
      case 2: result = "two"; break;
      default: result = "other"; break;
  }
  ```
- **Text blocks (triple quotes)** -> Use string concatenation or `String.join`
- **`var` keyword** -> Replace with explicit types
- **Enhanced switch with arrow syntax** -> Use traditional colon-case syntax

### API Differences
- **`List.of()`, `Set.of()`, `Map.of()`** -> Use `Collections.unmodifiableList(Arrays.asList(...))`, `Collections.unmodifiableSet(new HashSet<>(Arrays.asList(...)))`, or manual map construction
- **`String.isBlank()`** -> Use `string.trim().isEmpty()`
- **`String.strip()`** -> Use `string.trim()`
- **`String.repeat(n)`** -> Use a loop or `String.join("", Collections.nCopies(n, str))`
- **`Optional.isEmpty()`** -> Use `!optional.isPresent()`
- **`Stream.toList()`** -> Use `.collect(Collectors.toList())`
- **`Map.entry()`** -> Use `new AbstractMap.SimpleEntry<>()`
- **`Files.readString()` / `Files.writeString()`** -> Use traditional I/O with `BufferedReader`/`BufferedWriter`

### Project-Specific Differences
- The `stage-jdk8` branch uses `aerospike-client-jdk8` artifact instead of `aerospike-client-jdk21`
- pom.xml uses `java.version=1.8` and `java.api=8` instead of `java.version=21`
- Some features like `estimateKeySize()` do not exist in the jdk8 branch
- Do NOT modify pom.xml java version settings - those are already correct on `stage-jdk8`

### Important
- Preserve the original commit messages when cherry-picking
- If a file does not exist on `stage-jdk8`, check if the functionality lives in a different file or should be skipped
- After all changes, run `mvn clean install` and fix any compilation errors
