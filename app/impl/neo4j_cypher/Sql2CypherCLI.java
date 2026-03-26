import org.neo4j.jdbc.translator.spi.Translator;
import org.neo4j.jdbc.translator.impl.SqlToCypherTranslatorFactory;
import java.util.*;

public class Sql2CypherCLI {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java Sql2CypherCLI \"SQL_QUERY\"");
            System.exit(1);
        }
        String sql = args[0];
        Translator translator = new SqlToCypherTranslatorFactory().create(new HashMap<>());
        try {
            String cypher = translator.translate(sql);
            System.out.println(cypher);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
        }
    }
}
