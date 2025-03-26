# Cursor Rules

## 1. Occam's Razor
When there are multiple ways to solve a problem, prefer the simpler solution.

### Examples:

#### ❌ Complex String Manipulation
```java
// Complex string parsing
String[] parts = json.substring(1, json.length() - 1).split(",");
String id = parts[0].split(":")[1].replace("\"", "");
String name = parts[1].split(":")[1].replace("\"", "");
```

#### ✅ Using a JSON Library
```java
// Simple JSON parsing using Jackson
ObjectMapper mapper = new ObjectMapper();
CustomerProfile profile = mapper.readValue(json, CustomerProfile.class);
```

#### ❌ Multiple Lists for Related Data
```java
List<Server> servers = new ArrayList<>();
List<NetworkEndpoint> endpoints = new ArrayList<>();
List<StorageNode> storageNodes = new ArrayList<>();
```

#### ✅ Single List with Related Components
```java
class ServerNode {
    final Server server;
    final NetworkEndpoint endpoint;
    final StorageNode storage;
}

List<ServerNode> serverNodes = new ArrayList<>();
```

## 2. Error Handling
Always handle errors appropriately and provide meaningful error messages.

### Examples:

#### ❌ Ignoring Exceptions
```java
try {
    processData();
} catch (Exception e) {
    // Ignoring the error
}
```

#### ✅ Proper Error Handling
```java
try {
    processData();
} catch (IOException e) {
    logger.error("Failed to process data: {}", e.getMessage());
    throw new ProcessingException("Data processing failed", e);
}
```

## 3. Code Organization
Keep related code together and separate concerns appropriately.

### Examples:

#### ❌ Mixed Responsibilities
```java
class DataProcessor {
    void processData() { ... }
    void validateData() { ... }
    void sendEmail() { ... }  // Unrelated to data processing
}
```

#### ✅ Single Responsibility
```java
class DataProcessor {
    void processData() { ... }
}

class DataValidator {
    void validateData() { ... }
}

class NotificationService {
    void sendEmail() { ... }
}
```

## 4. Naming Conventions
Use clear, descriptive names that indicate purpose and intent.

### Examples:

#### ❌ Unclear Names
```java
void p() { ... }
String x = "data";
List<Object> l = new ArrayList<>();
```

#### ✅ Clear Names
```java
void processCustomerData() { ... }
String customerId = "data";
List<Customer> customers = new ArrayList<>();
```

## 5. Documentation
Document complex logic and public APIs.

### Examples:

#### ❌ Missing Documentation
```java
public void process(Data data) {
    // Complex logic without explanation
    if (data.isValid()) {
        transform(data);
        store(data);
    }
}
```

#### ✅ Clear Documentation
```java
/**
 * Processes customer data by validating, transforming, and storing it.
 * 
 * @param data The customer data to process
 * @throws ProcessingException if data validation fails
 */
public void process(Data data) {
    if (data.isValid()) {
        transform(data);
        store(data);
    } else {
        throw new ProcessingException("Invalid data");
    }
}
```

## 6. Testing
Write tests for all new functionality and maintain existing tests.

### Examples:

#### ❌ Missing Tests
```java
public class DataProcessor {
    public void processData() {
        // Complex logic without tests
    }
}
```

#### ✅ With Tests
```java
public class DataProcessor {
    public void processData() {
        // Implementation
    }
}

class DataProcessorTest {
    @Test
    void shouldProcessValidData() {
        // Test implementation
    }
    
    @Test
    void shouldHandleInvalidData() {
        // Test implementation
    }
}
```

## 7. Performance
Consider performance implications when choosing data structures and algorithms.

### Examples:

#### ❌ Inefficient Data Structure
```java
// Using ArrayList for frequent insertions/deletions
List<String> items = new ArrayList<>();
items.add(0, "new");  // O(n) operation
```

#### ✅ Efficient Data Structure
```java
// Using LinkedList for frequent insertions/deletions
List<String> items = new LinkedList<>();
items.add(0, "new");  // O(1) operation
```

## 8. Security
Follow security best practices when handling sensitive data.

### Examples:

#### ❌ Insecure Password Handling
```java
String password = "hardcoded_password";
```

#### ✅ Secure Password Handling
```java
// Using environment variables or secure configuration
String password = System.getenv("DB_PASSWORD");
```

## 9. Resource Management
Properly manage resources and use try-with-resources when possible.

### Examples:

#### ❌ Manual Resource Management
```java
FileInputStream fis = null;
try {
    fis = new FileInputStream("file.txt");
    // Use fis
} finally {
    if (fis != null) {
        fis.close();
    }
}
```

#### ✅ Using Try-With-Resources
```java
try (FileInputStream fis = new FileInputStream("file.txt")) {
    // Use fis
}
```

## 10. Code Duplication
Avoid code duplication by extracting common functionality.

### Examples:

#### ❌ Duplicated Code
```java
class UserValidator {
    void validateEmail(String email) {
        if (!email.contains("@")) {
            throw new ValidationException("Invalid email");
        }
    }
    
    void validatePhone(String phone) {
        if (!phone.contains("-")) {
            throw new ValidationException("Invalid phone");
        }
    }
}
```

#### ✅ Extracted Common Logic
```java
class UserValidator {
    void validateEmail(String email) {
        validateFormat(email, "@", "Invalid email");
    }
    
    void validatePhone(String phone) {
        validateFormat(phone, "-", "Invalid phone");
    }
    
    private void validateFormat(String value, String delimiter, String errorMessage) {
        if (!value.contains(delimiter)) {
            throw new ValidationException(errorMessage);
        }
    }
}
``` 