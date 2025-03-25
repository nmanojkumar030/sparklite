package minispark.deltalite;

import java.util.Locale;
import java.util.Optional;

public class DeltaSourceUtils {
    public static final String NAME = "deltalite";
    public static final String ALT_NAME = "deltalite";
    
    public static boolean isDeltaDataSourceName(String name) {
        if (name == null) return false;
        String lowerName = name.toLowerCase(Locale.ROOT);
        return lowerName.equals(NAME) || lowerName.equals(ALT_NAME);
    }
    
    public static boolean isDeltaTable(Optional<String> provider) {
        return provider.map(DeltaSourceUtils::isDeltaDataSourceName)
                      .orElse(false);
    }
} 